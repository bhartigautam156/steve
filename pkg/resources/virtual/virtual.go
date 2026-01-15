// Package virtual provides functions/resources to define virtual fields (fields which don't exist in k8s
// but should be visible in the API) on resources
package virtual

import (
	"fmt"
	"slices"
	"time"

	rescommon "github.com/rancher/steve/pkg/resources/common"
	"github.com/rancher/steve/pkg/resources/virtual/clusters"
	"github.com/rancher/steve/pkg/resources/virtual/common"
	"github.com/rancher/steve/pkg/resources/virtual/events"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

var now = time.Now

// TransformBuilder builds transform functions for specified GVKs through GetTransformFunc
type TransformBuilder struct {
	defaultFields *common.DefaultFields
}

// NewTransformBuilder returns a TransformBuilder using the given summary cache
func NewTransformBuilder(cache common.SummaryCache) *TransformBuilder {
	return &TransformBuilder{
		defaultFields: &common.DefaultFields{
			Cache: cache,
		},
	}
}

// GetTransformFunc returns the func to transform a raw object into a fixed object, if needed
func (t *TransformBuilder) GetTransformFunc(gvk schema.GroupVersionKind, columns []rescommon.ColumnDefinition, isCRD bool) cache.TransformFunc {
	converters := make([]func(*unstructured.Unstructured) (*unstructured.Unstructured, error), 0)
	if gvk.Kind == "Event" && gvk.Group == "" && gvk.Version == "v1" {
		converters = append(converters, events.TransformEventObject)
	} else if gvk.Kind == "Cluster" && gvk.Group == "management.cattle.io" && gvk.Version == "v3" {
		converters = append(converters, clusters.TransformManagedCluster)
	}

	//aded for debugging purposes
	for idx, col := range columns {
		logrus.Infof("Column %d: Name=%s, Field=%s, Type=%s", idx, col.Name, col.Field, col.Type)
	}

	// Detecting if we need to convert date fields
	for _, col := range columns {
		gvkDateFields, gvkFound := rescommon.DateFieldsByGVK[gvk]
		hasCRDDate := isCRD && col.Type == "date"
		hasBuiltInDate := gvkFound && slices.Contains(gvkDateFields, col.Name)

		if hasCRDDate || hasBuiltInDate {
			converters = append(converters, func(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
				logrus.Infof("Processing object: %+v", obj.Object)
				// FIX: Removed dependency on static 'index' from col.Field
				// because K8s 1.35+ may shift columns, making the schema index invalid.

				curValue, got, err := unstructured.NestedSlice(obj.Object, "metadata", "fields")
				if err != nil || !got {
					return obj, err
				}

				//for debugging purposes
				logrus.Infof("Fields array has %d items: %+v", len(curValue), curValue)

				// DYNAMIC SEARCH: Iterate over the row to find the "Age" column
				var found bool
				for i, field := range curValue {
					if field == nil {
						continue
					}

					// 1. Safety Cast
					valStr, ok := field.(string)
					if !ok {
						continue
					}

					// 2. Optimization: Skip strings that are clearly not durations.
					// "Age" strings (10d, 4h) start with a digit.
					// Names/Status (Active, local) start with letters.
					if len(valStr) == 0 || (valStr[0] < '0' || valStr[0] > '9') {
						continue
					}

					// 3. Attempt Parse
					duration, err := rescommon.ParseTimestampOrHumanReadableDuration(valStr)
					if err != nil {
						// If it fails to parse, it's just some other number/string. Keep looking.
						continue
					}

					// 4. FOUND IT: Update the value at THIS index (i)
					curValue[i] = fmt.Sprintf("%d", now().Add(-duration).UnixMilli())
					found = true

					// Stop after finding the first valid duration to avoid double-processing
					break
				}

				// If we didn't find a date, we simply exit without error so the UI shows whatever string is there
				if !found {
					// Optional: logrus.Debug("No valid duration field found in row")
					return obj, nil
				}

				// Write the updated slice back to the object
				if err := unstructured.SetNestedSlice(obj.Object, curValue, "metadata", "fields"); err != nil {
					return obj, err
				}

				return obj, nil
			})
		}
	}

	converters = append(converters, t.defaultFields.TransformCommon)

	return func(raw interface{}) (interface{}, error) {
		obj, isSignal, err := common.GetUnstructured(raw)
		if isSignal {
			return raw, err
		}
		if err != nil {
			return nil, fmt.Errorf("GetUnstructured: failed to get underlying object: %w", err)
		}
		for _, f := range converters {
			transformed, err := f(obj)
			if err != nil {
				logrus.Errorf("error in transform for gvk Kind:%s Group:%s Version:%s, error: %v", gvk.Kind, gvk.Group, gvk.Version, err)
			}
			obj = transformed
		}
		return obj, nil
	}
}
