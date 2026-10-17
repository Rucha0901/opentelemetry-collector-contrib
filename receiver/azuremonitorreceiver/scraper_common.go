// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azuremonitorreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azuremonitorreceiver"

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions/v2"
	"github.com/huandu/go-clone"
	"go.uber.org/zap"
)

var (
	timeGrains = map[string]int64{
		"PT1M":  60,
		"PT5M":  300,
		"PT15M": 900,
		"PT30M": 1800,
		"PT1H":  3600,
		"PT6H":  21600,
		"PT12H": 43200,
		"P1D":   86400,
	}
	aggregations = []string{
		"Average",
		"Count",
		"Maximum",
		"Minimum",
		"Total",
	}
)

const (
	attributeLocation      = "location"
	attributeName          = "name"
	attributeResourceGroup = "resource_group"
	attributeResourceType  = "type"
	attributeTimeGrain     = "timegrain"
	metadataPrefix         = "metadata_"
	tagPrefix              = "tags_"
	truncateTimeGrain      = time.Minute
	filterAllAggregations  = "*"
	storageAccountType     = "Microsoft.Storage/storageAccounts"
)

// azureSubscription is an extract of armsubscriptions.Subscription.
// It designates a common structure between complex structures retrieved from the API
// and simple subscriptions ids that you can find in config.
type azureSubscription struct {
	SubscriptionID   string
	DisplayName      string
	resourcesUpdated time.Time
}

type azureResource struct {
	attributes                map[string]*string
	metricsByCompositeKey     map[metricsCompositeKey]*azureResourceMetrics
	metricsDefinitionsUpdated time.Time
	tags                      map[string]*string
	resourceType              *string
}

type metricsCompositeKey struct {
	namespace    string
	dimensions   string // comma separated sorted dimensions
	aggregations string // comma separated sorted aggregations
	timeGrain    string
}

type azureResourceMetrics struct {
	metrics               []string
	metricsValuesUpdated  time.Time
	lastEmittedTimestamps map[string]time.Time
}

type void struct{}

type timeNowIface interface {
	Now() time.Time
}

type timeWrapper struct{}

func (*timeWrapper) Now() time.Time {
	return time.Now()
}

type storageAccountSpecificConfig struct {
	askedBlobServices  bool
	askedFileServices  bool
	askedQueueServices bool
	askedTableServices bool
}

func newStorageAccountSpecificConfig(services []string) storageAccountSpecificConfig {
	return storageAccountSpecificConfig{
		askedBlobServices:  slices.IndexFunc(services, func(s string) bool { return strings.EqualFold(s, "Microsoft.Storage/storageAccounts/blobServices") }) != -1,
		askedFileServices:  slices.IndexFunc(services, func(s string) bool { return strings.EqualFold(s, "Microsoft.Storage/storageAccounts/fileServices") }) != -1,
		askedQueueServices: slices.IndexFunc(services, func(s string) bool { return strings.EqualFold(s, "Microsoft.Storage/storageAccounts/queueServices") }) != -1,
		askedTableServices: slices.IndexFunc(services, func(s string) bool { return strings.EqualFold(s, "Microsoft.Storage/storageAccounts/tableServices") }) != -1,
	}
}

func loadSubscriptionsCommon(
	ctx context.Context,
	logger *zap.Logger,
	cfg *Config,
	cred azcore.TokenCredential,
	clientOptions ClientOptionsResolver,
	subscriptionsUpdated *time.Time,
	subscriptions map[string]*azureSubscription,
	loadSub func(azureSubscription),
	unloadSub func(string),
) {
	logger.Debug("Loading the list of Azure Subscriptions", zap.Bool("discover_subscriptions", cfg.DiscoverSubscriptions))
	if time.Since(*subscriptionsUpdated).Seconds() < cfg.CacheResources {
		logger.Debug("Azure subscriptions are cached, skipping refresh")
		return
	}

	armSubscriptionClient, clientErr := armsubscriptions.NewClient(cred, clientOptions.GetArmSubscriptionsClientOptions())
	if clientErr != nil {
		logger.Error("Failed to initialize the client for Azure Subscriptions",
			zap.Error(clientErr))
		return
	}

	// Make a special case for when we only have subscription ids configured (discovery disabled)
	if !cfg.DiscoverSubscriptions {
		for _, subID := range cfg.SubscriptionIDs {
			if !cfg.MetricsBuilderConfig.ResourceAttributes.AzuremonitorSubscription.Enabled {
				loadSub(azureSubscription{
					SubscriptionID: subID,
				})
				continue
			}

			resp, err := armSubscriptionClient.Get(ctx, subID, &armsubscriptions.ClientGetOptions{})
			logFields := []zap.Field{zap.String("subscription_id", subID)}
			if err != nil {
				logFields = append(logFields, zap.Error(err))
				logger.Error("Failed to collect Subscription info from Azure", logFields...)
				return
			}
			logFields = append(logFields, zap.String("subscription_display_name", *resp.DisplayName))
			logger.Debug("Collected Subscription info from Azure", logFields...)
			loadSub(azureSubscription{
				SubscriptionID: *resp.SubscriptionID,
				DisplayName:    *resp.DisplayName,
			})
		}
		*subscriptionsUpdated = time.Now()
		logger.Info("Loaded the list of Azure Subscriptions",
			zap.Int("subscriptions_count", len(subscriptions)))
		return
	}

	// Prepare a map of existing subscriptions to detect removed ones later
	existingSubscriptions := map[string]void{}
	for id := range subscriptions {
		existingSubscriptions[id] = void{}
	}

	opts := &armsubscriptions.ClientListOptions{}
	pager := armSubscriptionClient.NewListPager(opts)
	page := 0
	for pager.More() {
		nextResult, err := pager.NextPage(ctx)
		logFields := []zap.Field{zap.Int("page", page)}
		if err != nil {
			logFields = append(logFields, zap.Error(err))
			logger.Error("Failed to collect Subscription list from Azure", logFields...)
			return
		}
		logFields = append(logFields, zap.Int("subscriptions_count", len(nextResult.Value)))
		logger.Debug("Collected Subscription list page from Azure", logFields...)
		page++

		for _, subscription := range nextResult.Value {
			loadSub(azureSubscription{
				SubscriptionID: *subscription.SubscriptionID,
				DisplayName:    *subscription.DisplayName,
			})
			delete(existingSubscriptions, *subscription.SubscriptionID)
		}
	}

	// Unload subscriptions that are no longer present
	if len(existingSubscriptions) > 0 {
		for idToDelete := range existingSubscriptions {
			unloadSub(idToDelete)
		}
	}

	*subscriptionsUpdated = time.Now()
	logger.Info("Loaded the list of Azure Subscriptions",
		zap.Int("subscriptions_count", len(subscriptions)),
		zap.Int("deleted_subscriptions_count", len(existingSubscriptions)))
}

// processResources is a workaround specially done for the storageAccount metrics.
// Every StorageAccount resources have some implicit sub resources (/blobServices/default, fileServices/default, etc...) that are not returned by the API.
// We need to add them manually to the resources and resourceTypes map.
// Note that we add these virtual sub resources only if the user has asked the sub resource types explicitly in the services config.
// Example:
// For each resource with id .../Microsoft.Storage/storageAccount/myResource of type Microsoft.Storage/storageAccount,
// It will create a virtual resource with id .../Microsoft.Storage/storageAccount/myResource/blobServices/default of type Microsoft.Storage/storageAccounts/blobServices.
func processResources(storageConfig storageAccountSpecificConfig, resources []*armresources.GenericResourceExpanded) []*armresources.GenericResourceExpanded {
	var subTypeResources []*armresources.GenericResourceExpanded
	for _, resource := range resources {
		subTypeResources = append(subTypeResources, resource)
		if resource != nil && resource.Type != nil && *resource.Type == storageAccountType {
			if storageConfig.askedBlobServices {
				if r := buildSubTypeResource(*resource, "Microsoft.Storage/storageAccounts/blobServices", fmt.Sprintf("%s/blobServices/default", *resource.ID)); r != nil {
					subTypeResources = append(subTypeResources, r)
				}
			}
			if storageConfig.askedFileServices {
				if r := buildSubTypeResource(*resource, "Microsoft.Storage/storageAccounts/fileServices", fmt.Sprintf("%s/fileServices/default", *resource.ID)); r != nil {
					subTypeResources = append(subTypeResources, r)
				}
			}
			if storageConfig.askedQueueServices {
				if r := buildSubTypeResource(*resource, "Microsoft.Storage/storageAccounts/queueServices", fmt.Sprintf("%s/queueServices/default", *resource.ID)); r != nil {
					subTypeResources = append(subTypeResources, r)
				}
			}
			if storageConfig.askedTableServices {
				if r := buildSubTypeResource(*resource, "Microsoft.Storage/storageAccounts/tableServices", fmt.Sprintf("%s/tableServices/default", *resource.ID)); r != nil {
					subTypeResources = append(subTypeResources, r)
				}
			}
		}
	}
	return subTypeResources
}

func getResourcesFilter(cfg *Config) string {
	// TODO: switch to parsing services from
	// https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/metrics-supported
	resourcesTypeFilter := strings.Join(cfg.Services, "' or resourceType eq '")

	resourcesGroupFilterString := ""
	if len(cfg.ResourceGroups) > 0 {
		resourcesGroupFilterString = fmt.Sprintf(" and (resourceGroup eq '%s')",
			strings.Join(cfg.ResourceGroups, "' or resourceGroup eq  '"))
	}

	return fmt.Sprintf("(resourceType eq '%s')%s", resourcesTypeFilter, resourcesGroupFilterString)
}

func buildMetricCompositeKey(
	v *armmonitor.MetricDefinition,
	cfg *Config,
	resourceType string,
	discoveredNamespaces map[string]struct{},
) (compositeKey metricsCompositeKey, metricName string, ok bool) {
	metricName = *v.Name.Value
	metricNamespace := *v.Namespace

	if discoveredNamespaces != nil {
		discoveredNamespaces[strings.ToLower(metricNamespace)] = struct{}{}
	}

	metricAggregations := getMetricAggregations(metricNamespace, metricName, cfg.Metrics, convertAggregationsToStr(v.SupportedAggregationTypes))
	if len(metricAggregations) == 0 {
		return metricsCompositeKey{}, "", false
	}

	timeGrain := getMetricTimeGrain(metricNamespace, metricName, cfg.TimeGrainOverrides, *v.MetricAvailabilities[0].TimeGrain)
	dimensions := filterDimensions(v.Dimensions, cfg.Dimensions, resourceType, metricName)
	compositeKey = metricsCompositeKey{
		namespace:    metricNamespace,
		timeGrain:    timeGrain,
		dimensions:   serializeDimensions(dimensions),
		aggregations: strings.Join(metricAggregations, ","),
	}
	return compositeKey, metricName, true
}

func collectCustomMetricNamespaces(
	cfg *Config,
	discoveredNamespaces map[string]struct{},
	collectFn func(opts *armmonitor.MetricDefinitionsClientListOptions),
) {
	for configNamespace := range cfg.Metrics {
		if _, found := discoveredNamespaces[strings.ToLower(configNamespace)]; found {
			continue
		}
		opts := &armmonitor.MetricDefinitionsClientListOptions{
			Metricnamespace: new(configNamespace),
		}
		collectFn(opts)
	}
}

func addMetricToCompositeKey(metricsByCompositeKey map[metricsCompositeKey]*azureResourceMetrics, metricName string, compositeKey metricsCompositeKey) {
	if _, ok := metricsByCompositeKey[compositeKey]; ok {
		metricsByCompositeKey[compositeKey].metrics = append(
			metricsByCompositeKey[compositeKey].metrics, metricName,
		)
	} else {
		metricsByCompositeKey[compositeKey] = &azureResourceMetrics{
			metrics:               []string{metricName},
			lastEmittedTimestamps: make(map[string]time.Time),
		}
	}
}

// buildSubTypeResource creates a virtual new resource with given type and ID.
// The rest of the attributes (location, tags, etc...) are copied from the original resource.
func buildSubTypeResource(orig armresources.GenericResourceExpanded, newType, newID string) *armresources.GenericResourceExpanded {
	cloned := clone.Clone(orig).(armresources.GenericResourceExpanded)
	cloned.ID = &newID
	cloned.Type = &newType
	return &cloned
}

func getResourceGroupFromID(id string) string {
	s := regexp.MustCompile(`/resourcegroups/([^/]+)/`)
	match := s.FindStringSubmatch(strings.ToLower(id))

	if len(match) == 2 {
		return match[1]
	}
	return ""
}

// getMetricAggregations returns a list of aggregations for a given namespace/metric.
// Two parameters are considered to know the aggregation to choose
// - a filter (given in configuration)
// - a list of supported aggregations (given by the API)
// If one namespace/metrics combination matches a provided filter,
// > Then it returns the aggregations in the filter
// > Otherwise it returns all supported aggregations.
// Note that a special filter * is supported to return all supported aggregations explicitly.
// /!\ It does not control the aggregations in the filters. If it's not in the supported list, it still lets it pass.
func getMetricAggregations(metricNamespace, metricName string, filters NestedListAlias, supportedAggregations []string) []string {
	// default behavior when no metric filters specified: pass all metrics with all aggregations
	if len(filters) == 0 {
		return supportedAggregations
	}

	metricsFilters, ok := mapFindInsensitive(filters, metricNamespace)
	// metric namespace isn't found, or it's empty: pass all metrics from the namespace
	if !ok || len(metricsFilters) == 0 {
		return supportedAggregations
	}

	aggregationsFilters, ok := mapFindInsensitive(metricsFilters, metricName)
	// if the target metric is absent in the metrics map: filter out metric
	if !ok {
		return []string{}
	}
	// allow all aggregations if others are not specified
	if len(aggregationsFilters) == 0 || slices.Contains(aggregationsFilters, filterAllAggregations) {
		return supportedAggregations
	}

	// collect known aggregations without filtering on supported
	var out []string
	for _, filter := range aggregationsFilters {
		for _, aggregation := range aggregations {
			if strings.EqualFold(aggregation, filter) {
				out = append(out, aggregation)
			}
		}
	}

	return out
}

func getMetricTimeGrain(metricNamespace, metricName string, overrides MetricTimeGrainOverrides, defaultTimeGrain string) string {
	namespaceOverrides, ok := mapFindInsensitive(overrides, metricNamespace)
	if !ok {
		return defaultTimeGrain
	}

	timeGrain, ok := mapFindInsensitive(namespaceOverrides, metricName)
	if !ok {
		return defaultTimeGrain
	}

	return strings.ToUpper(timeGrain)
}

func convertAggregationsToStr(aggregations []*armmonitor.AggregationType) []string {
	var result []string
	for _, aggr := range aggregations {
		result = append(result, string(*aggr))
	}
	return result
}

func mapFindInsensitive[T any](m map[string]T, key string) (T, bool) {
	for k, v := range m {
		if strings.EqualFold(key, k) {
			return v, true
		}
	}

	var got T
	return got, false
}

// getTagsFilterMap returns a map used to filter tags.
// Each user-configured tag key is normalized to lowercase and added to the map for case-insensitive lookup.
func getTagsFilterMap(appendTagsAsAttributes []string) (tagsFilterMap map[string]struct{}) {
	tagsFilterMap = make(map[string]struct{}, len(appendTagsAsAttributes))
	for _, v := range appendTagsAsAttributes {
		tagsFilterMap[strings.ToLower(v)] = struct{}{}
	}
	return tagsFilterMap
}

// filterResourceTags filter out resource tags according to configured tag list (append_tags_as_attributes)
func filterResourceTags(tagFilterList map[string]struct{}, resourceTags map[string]*string) map[string]*string {
	if _, includeAll := tagFilterList["*"]; includeAll {
		return resourceTags
	}

	// wildcard not found. include only configured tags
	includedTags := make(map[string]*string, len(resourceTags))
	for tagName, value := range resourceTags {
		if _, ok := tagFilterList[strings.ToLower(tagName)]; ok {
			includedTags[tagName] = value
		}
	}

	return includedTags
}
