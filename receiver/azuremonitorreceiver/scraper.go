// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azuremonitorreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azuremonitorreceiver"

import (
	"context"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources/v4"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azuremonitorreceiver/internal/metadata"
)

func newScraper(conf *Config, settings receiver.Settings) *azureScraper {
	return &azureScraper{
		cfg:                          conf,
		settings:                     settings.TelemetrySettings,
		mb:                           metadata.NewMetricsBuilder(conf.MetricsBuilderConfig, settings),
		mutex:                        &sync.Mutex{},
		time:                         &timeWrapper{},
		clientOptionsResolver:        newClientOptionsResolver(conf.Cloud),
		storageAccountSpecificConfig: newStorageAccountSpecificConfig(conf.Services),
	}
}

type azureScraper struct {
	cred azcore.TokenCredential

	cfg      *Config
	settings component.TelemetrySettings
	// resources on which we'll collect metrics. Stored by resource id and subscription id.
	resources map[string]map[string]*azureResource
	// subscriptions on which we'll look up resources. Stored by subscription id.
	subscriptions        map[string]*azureSubscription
	subscriptionsUpdated time.Time
	mb                   *metadata.MetricsBuilder

	mutex                        *sync.Mutex
	time                         timeNowIface
	clientOptionsResolver        ClientOptionsResolver
	storageAccountSpecificConfig storageAccountSpecificConfig
}

func (s *azureScraper) start(_ context.Context, host component.Host) (err error) {
	if s.cred, err = loadCredentials(s.settings.Logger, s.cfg, host); err != nil {
		return err
	}

	s.subscriptions = map[string]*azureSubscription{}
	s.resources = map[string]map[string]*azureResource{}

	return err
}

func (s *azureScraper) loadSubscription(sub azureSubscription) {
	s.resources[sub.SubscriptionID] = make(map[string]*azureResource)
	s.subscriptions[sub.SubscriptionID] = &azureSubscription{
		SubscriptionID: sub.SubscriptionID,
		DisplayName:    sub.DisplayName,
	}
}

func (s *azureScraper) unloadSubscription(id string) {
	s.settings.Logger.Debug("Unloading subscription", zap.String("subscription_id", id))
	delete(s.resources, id)
	delete(s.subscriptions, id)
}

func (s *azureScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	s.loadSubscriptions(ctx)

	for subscriptionID, subscription := range s.subscriptions {
		s.loadResources(ctx, subscriptionID)

		// Collect metrics values per resource sequentially
		// (this is not supported in batch by the metrics API)
		resourcesWithDefinitions := make(chan string)
		go func() {
			defer close(resourcesWithDefinitions)
			for resourceID := range s.resources[subscriptionID] {
				s.loadMetricsDefinitions(ctx, subscriptionID, resourceID)
				resourcesWithDefinitions <- resourceID
			}
		}()

		var wg sync.WaitGroup
		for resourceID := range resourcesWithDefinitions {
			wg.Add(1)
			go func(resID string) {
				defer wg.Done()
				s.loadMetricsValues(ctx, subscriptionID, resID)
			}(resourceID)
		}

		wg.Wait()

		rb := s.mb.NewResourceBuilder()
		rb.SetAzuremonitorTenantID(s.cfg.TenantID)
		rb.SetAzuremonitorSubscriptionID(subscriptionID)
		rb.SetAzuremonitorSubscription(subscription.DisplayName)

		s.mb.EmitForResource(metadata.WithResource(rb.Emit()))
	}
	return s.mb.Emit(), nil
}

func (s *azureScraper) loadSubscriptions(ctx context.Context) {
	loadSubscriptionsCommon(
		ctx,
		s.settings.Logger,
		s.cfg,
		s.cred,
		s.clientOptionsResolver,
		&s.subscriptionsUpdated,
		s.subscriptions,
		s.loadSubscription,
		s.unloadSubscription,
	)
}

func (s *azureScraper) loadResources(ctx context.Context, subscriptionID string) {
	s.settings.Logger.Debug("Loading the list of Azure Resources",
		zap.String("subscription_id", subscriptionID))
	if time.Since(s.subscriptions[subscriptionID].resourcesUpdated).Seconds() < s.cfg.CacheResources {
		s.settings.Logger.Debug("Azure Resources are cached, skipping refresh",
			zap.String("subscription_id", subscriptionID))
		return
	}

	clientResources, clientErr := armresources.NewClient(subscriptionID, s.cred, s.clientOptionsResolver.GetArmResourceClientOptions(subscriptionID))
	if clientErr != nil {
		s.settings.Logger.Error("Failed to initialize the client for Azure Resources",
			zap.String("subscription_id", subscriptionID),
			zap.Error(clientErr))
		return
	}

	// Prepare a map of existing resources to detect removed ones later
	existingResources := map[string]void{}
	for id := range s.resources[subscriptionID] {
		existingResources[id] = void{}
	}

	// Prepare the tags filter to apply on resources later on.
	// TODO: We don't need to do it per subscription. It can be done upper in the code to improve performances.
	tagsFilterMap := getTagsFilterMap(s.cfg.AppendTagsAsAttributes)

	// Prepare the options to get the resource list.
	filter := s.getResourcesFilter()
	opts := &armresources.ClientListOptions{
		Filter: &filter,
	}

	pager := clientResources.NewListPager(opts)
	page := 0
	for pager.More() {
		nextResult, err := pager.NextPage(ctx)

		logFields := []zap.Field{
			zap.String("subscription_id", subscriptionID),
			zap.String("filter", filter),
			zap.Int("page", page),
		}
		if err != nil {
			logFields = append(logFields, zap.Error(err))
			s.settings.Logger.Error("Failed to collect Resource list from Azure", logFields...)
			return
		}
		logFields = append(logFields, zap.Int("resources_count", len(nextResult.Value)))
		s.settings.Logger.Debug("Collected Resource list from Azure", logFields...)
		page++

		resources := filterResourcesByTags(nextResult.Value, s.cfg.ResourceTags)

		for _, resource := range s.processResources(resources) {
			if _, ok := s.resources[subscriptionID][*resource.ID]; !ok {
				resourceGroup := getResourceGroupFromID(*resource.ID)
				attributes := map[string]*string{
					attributeName:          resource.Name,
					attributeResourceGroup: &resourceGroup,
					attributeResourceType:  resource.Type,
				}
				if resource.Location != nil {
					attributes[attributeLocation] = resource.Location
				}
				s.resources[subscriptionID][*resource.ID] = &azureResource{
					attributes:   attributes,
					tags:         filterResourceTags(tagsFilterMap, resource.Tags),
					resourceType: resource.Type,
				}
			}
			delete(existingResources, *resource.ID)
		}
	}

	// Unload resources that are no longer present
	if len(existingResources) > 0 {
		for idToDelete := range existingResources {
			delete(s.resources[subscriptionID], idToDelete)
		}
	}

	s.subscriptions[subscriptionID].resourcesUpdated = time.Now()
	s.settings.Logger.Info("Loaded the list of Azure Resources",
		zap.String("subscription_id", subscriptionID),
		zap.Int("resources_count", len(s.resources[subscriptionID])),
		zap.Int("deleted_resources_count", len(existingResources)))
}

func (s *azureScraper) processResources(resources []*armresources.GenericResourceExpanded) []*armresources.GenericResourceExpanded {
	return processResources(s.storageAccountSpecificConfig, resources)
}

func (s *azureScraper) getResourcesFilter() string {
	return getResourcesFilter(s.cfg)
}

func (s *azureScraper) loadMetricsDefinitions(ctx context.Context, subscriptionID, resourceID string) {
	s.settings.Logger.Debug("Loading the list of Azure Metrics Definitions",
		zap.String("resource_id", resourceID),
		zap.String("subscription_id", subscriptionID))
	if time.Since(s.resources[subscriptionID][resourceID].metricsDefinitionsUpdated).Seconds() < s.cfg.CacheResourcesDefinitions {
		s.settings.Logger.Debug("Azure Metrics Definitions are cached, skipping refresh",
			zap.String("resource_id", resourceID),
			zap.String("subscription_id", subscriptionID))
		return
	}

	// Prepare the map of metrics by composite key.
	s.resources[subscriptionID][resourceID].metricsByCompositeKey = map[metricsCompositeKey]*azureResourceMetrics{}

	clientMetricsDefinitions, clientErr := armmonitor.NewMetricDefinitionsClient(subscriptionID, s.cred, s.clientOptionsResolver.GetArmMonitorClientOptions())
	if clientErr != nil {
		s.settings.Logger.Error("Failed to initialize the client for Azure Metrics definitions",
			zap.Error(clientErr))
		return
	}

	// discoveredNamespaces tracks namespaces returned by the default (no-filter) call
	// so we can skip redundant namespace-filtered calls for those namespaces.
	discoveredNamespaces := map[string]struct{}{}

	s.collectMetricDefinitions(ctx, subscriptionID, resourceID, clientMetricsDefinitions, nil, discoveredNamespaces)

	// The Azure Monitor MetricDefinitions API only returns custom metric namespace
	// definitions (e.g. "azure.vm.linux.guestmetrics" published by AMA/MetricsExtension)
	// when the metricnamespace query parameter is set explicitly. Make additional calls
	// for each namespace configured in the metrics filter that was not already returned
	// by the default call above.
	collectCustomMetricNamespaces(s.cfg, discoveredNamespaces, func(opts *armmonitor.MetricDefinitionsClientListOptions) {
		s.collectMetricDefinitions(ctx, subscriptionID, resourceID, clientMetricsDefinitions, opts, nil)
	})

	s.resources[subscriptionID][resourceID].metricsDefinitionsUpdated = time.Now()
	s.settings.Logger.Info("Loaded the list of Azure Metrics Definitions",
		zap.Int("metrics_definitions_count", len(s.resources[subscriptionID][resourceID].metricsByCompositeKey)),
		zap.String("resource_id", resourceID),
		zap.String("subscription_id", subscriptionID))
}

// collectMetricDefinitions pages through a MetricDefinitions pager and registers each
// metric definition into the resource's metricsByCompositeKey map.
// discoveredNamespaces, when non-nil, is populated with the lowercased namespaces seen in
// the response so callers can skip redundant follow-up calls.
func (s *azureScraper) collectMetricDefinitions(
	ctx context.Context,
	subscriptionID, resourceID string,
	client *armmonitor.MetricDefinitionsClient,
	opts *armmonitor.MetricDefinitionsClientListOptions,
	discoveredNamespaces map[string]struct{},
) {
	pager := client.NewListPager(resourceID, opts)
	page := 0
	for pager.More() {
		nextResult, err := pager.NextPage(ctx)

		logFields := []zap.Field{
			zap.String("resource_id", resourceID),
			zap.String("subscription_id", subscriptionID),
			zap.Int("page", page),
		}
		if err != nil {
			logFields = append(logFields, zap.Error(err))
			s.settings.Logger.Error("Failed to collect Azure Definitions list from Azure", logFields...)
			return
		}
		logFields = append(logFields, zap.Int("definitions_count", len(nextResult.Value)))
		s.settings.Logger.Debug("Collected Azure Metrics Definitions list from Azure", logFields...)
		page++

		for _, v := range nextResult.Value {
			compositeKey, metricName, ok := buildMetricCompositeKey(v, s.cfg, *s.resources[subscriptionID][resourceID].resourceType, discoveredNamespaces)
			if !ok {
				continue
			}
			s.loadMetricsDefinition(subscriptionID, resourceID, metricName, compositeKey)
		}
	}
}

func (s *azureScraper) loadMetricsDefinition(subscriptionID, resourceID, metricName string, compositeKey metricsCompositeKey) {
	s.settings.Logger.Debug("Loading metric definition",
		zap.String("dimensions", compositeKey.dimensions),
		zap.String("aggregations", compositeKey.aggregations),
		zap.String("timegrain", compositeKey.timeGrain),
		zap.String("metric", metricName),
		zap.String("resource_id", resourceID),
		zap.String("subscription_id", subscriptionID))
	addMetricToCompositeKey(s.resources[subscriptionID][resourceID].metricsByCompositeKey, metricName, compositeKey)
}

func (s *azureScraper) loadMetricsValues(ctx context.Context, subscriptionID, resourceID string) {
	s.settings.Logger.Debug("Loading the Azure Metrics",
		zap.String("resource_id", resourceID),
		zap.String("subscription_id", subscriptionID))
	res := *s.resources[subscriptionID][resourceID]
	updatedAt := s.time.Now().Truncate(truncateTimeGrain)

	clientMetricsValues, clientErr := armmonitor.NewMetricsClient(subscriptionID, s.cred, s.clientOptionsResolver.GetArmMonitorClientOptions())
	if clientErr != nil {
		s.settings.Logger.Error("Failed to initialize the client for Azure Metrics",
			zap.String("resource_id", resourceID),
			zap.String("subscription_id", subscriptionID),
			zap.Error(clientErr))
		return
	}

	for compositeKey, metricsByGrain := range res.metricsByCompositeKey {
		if updatedAt.Sub(metricsByGrain.metricsValuesUpdated).Seconds() < float64(timeGrains[compositeKey.timeGrain]) {
			continue
		}
		metricsByGrain.metricsValuesUpdated = updatedAt

		start := 0

		for start < len(metricsByGrain.metrics) {
			end := min(start+s.cfg.MaximumNumberOfMetricsInACall, len(metricsByGrain.metrics))

			opts := newResourceMetricsValuesRequestOptions(
				metricsByGrain.metrics,
				compositeKey.dimensions,
				compositeKey.timeGrain,
				compositeKey.aggregations,
				compositeKey.namespace,
				start,
				end,
				s.cfg.MaximumNumberOfRecordsPerResource,
			)

			result, err := clientMetricsValues.List(
				ctx,
				resourceID,
				&opts,
			)
			logFields := []zap.Field{
				zap.Any("metrics", metricsByGrain.metrics[start:end]),
				zap.String("metrics_namespace", compositeKey.namespace),
				zap.String("dimensions", compositeKey.dimensions),
				zap.String("aggregations", compositeKey.aggregations),
				zap.String("timegrain", compositeKey.timeGrain),
				zap.String("resource_id", resourceID),
				zap.String("subscription_id", subscriptionID),
			}
			if err != nil {
				logFields = append(logFields, zap.Error(err))
				s.settings.Logger.Error("Failed to collect Azure Metrics values from Azure", logFields...)
				return
			}

			start = end

			logFields = append(logFields, zap.Int("metrics_count", len(result.Value)))
			s.settings.Logger.Debug("Collected Azure Metrics values from Azure", logFields...)

			if metricsByGrain.lastEmittedTimestamps == nil {
				metricsByGrain.lastEmittedTimestamps = make(map[string]time.Time)
			}

			for _, metric := range result.Value {
				for _, timeseriesElement := range metric.Timeseries {
					if timeseriesElement.Data == nil {
						continue
					}
					attributes := map[string]*string{}
					maps.Copy(attributes, res.attributes)
					for _, value := range timeseriesElement.Metadatavalues {
						name := metadataPrefix + *value.Name.Value
						attributes[name] = value.Value
					}
					for tagName, value := range res.tags {
						name := tagPrefix + tagName
						attributes[name] = value
					}
					attributes[attributeTimeGrain] = &compositeKey.timeGrain

					var metricName string
					if metric.Name != nil && metric.Name.Value != nil {
						metricName = *metric.Name.Value
					}
					tsKeyPrefix := resourceID + "#" + metricName + "#" + serializeArmMetadataValues(timeseriesElement.Metadatavalues)

					for _, metricValue := range timeseriesElement.Data {
						if metricValue == nil || metricValue.TimeStamp == nil {
							continue
						}
						t := *metricValue.TimeStamp

						lastTime, exists := metricsByGrain.lastEmittedTimestamps[tsKeyPrefix]
						if !exists || t.After(lastTime) {
							pts := pcommon.NewTimestampFromTime(t)
							s.processTimeseriesData(resourceID, metric, metricValue, attributes, pts)
							metricsByGrain.lastEmittedTimestamps[tsKeyPrefix] = t
						}
					}
				}
			}
		}
	}
	s.settings.Logger.Info("Loaded the Azure Metrics",
		zap.String("resource_id", resourceID),
		zap.String("subscription_id", subscriptionID))
}

func newResourceMetricsValuesRequestOptions(
	metrics []string,
	dimensionsStr string,
	timeGrain string,
	aggregationsStr string,
	namespace string,
	start int,
	end int,
	top int32,
) armmonitor.MetricsClientListOptions {
	opts := armmonitor.MetricsClientListOptions{
		Metricnames: new(strings.Join(metrics[start:end], ",")),
		Interval:    new(timeGrain),
		Timespan:    new(timeGrain),
		Aggregation: new(aggregationsStr),
		Top:         new(top),
		Filter:      buildDimensionsFilter(dimensionsStr),
	}
	if namespace != "" {
		opts.Metricnamespace = new(namespace)
	}
	return opts
}

func (s *azureScraper) processTimeseriesData(
	resourceID string,
	metric *armmonitor.Metric,
	metricValue *armmonitor.MetricValue,
	attributes map[string]*string,
	ts pcommon.Timestamp,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	aggregationsData := []struct {
		name  string
		value *float64
	}{
		{"Average", metricValue.Average},
		{"Count", metricValue.Count},
		{"Maximum", metricValue.Maximum},
		{"Minimum", metricValue.Minimum},
		{"Total", metricValue.Total},
	}
	for _, aggregation := range aggregationsData {
		if aggregation.value != nil {
			s.mb.AddDataPoint(
				resourceID,
				*metric.Name.Value,
				aggregation.name,
				string(*metric.Unit),
				attributes,
				ts,
				*aggregation.value,
			)
		}
	}
}

func serializeArmMetadataValues(metadataValues []*armmonitor.MetadataValue) string {
	if len(metadataValues) == 0 {
		return ""
	}
	parts := make([]string, 0, len(metadataValues))
	for _, val := range metadataValues {
		if val.Name != nil && val.Name.Value != nil && val.Value != nil {
			parts = append(parts, *val.Name.Value+"="+*val.Value)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
