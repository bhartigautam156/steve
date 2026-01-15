// Package virtual provides functions/resources to define virtual fields (fields which don't exist in k8s
// but should be visible in the API) on resources
package virtual

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
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

	// Detecting if we need to convert date fields
	for _, col := range columns {
		gvkDateFields, gvkFound := rescommon.DateFieldsByGVK[gvk]
		hasCRDDate := isCRD && col.Type == "date"
		hasBuiltInDate := gvkFound && slices.Contains(gvkDateFields, col.Name)

		if hasCRDDate || hasBuiltInDate {
			// Extract the index from col.Field (e.g., "$.metadata.fields[2]" -> 2)
			fieldIndex := extractFieldIndex(col.Field)

			// Capture fieldIndex in closure
			idx := fieldIndex

			converters = append(converters, func(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
				curValue, got, err := unstructured.NestedSlice(obj.Object, "metadata", "fields")
				if err != nil || !got {
					logrus.Debugf("couldn't find metadata.fields at unstr.Object")
					return obj, err
				}

				// Check if the index is within bounds
				if idx < 0 || idx >= len(curValue) {
					logrus.Debugf("Field index %d out of bounds (length: %d)", idx, len(curValue))
					return obj, nil
				}

				field := curValue[idx]
				if field == nil {
					logrus.Debugf("Field at index %d is nil", idx)
					return obj, nil
				}

				// Safety cast
				valStr, ok := field.(string)
				if !ok {
					logrus.Warnf("time field isn't a string")
					return obj, nil
				}

				// Parse the duration
				duration, err := rescommon.ParseTimestampOrHumanReadableDuration(valStr)
				if err != nil {
					logrus.Debugf("convert timestamp value: %s failed with error: %v", valStr, err)
					return obj, nil
				}

				// Update the value at the correct index
				curValue[idx] = fmt.Sprintf("%d", now().Add(-duration).UnixMilli())

				// Write back
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

// extractFieldIndex extracts the index from a field path like "$.metadata.fields[2]"
// Returns -1 if the index cannot be extracted
func extractFieldIndex(fieldPath string) int {
	// Extract number from "$.metadata.fields[2]"
	re := regexp.MustCompile(`\[(\d+)\]`)
	matches := re.FindStringSubmatch(fieldPath)
	if len(matches) < 2 {
		return -1
	}
	idx, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1
	}
	return idx
}
