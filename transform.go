package statistica

import "fmt"

type keyUnion string

// UnionItemsResponse union data struct ItemsResponse.
func UnionItemsResponse(response ...*ItemsResponse) *ItemsResponse {
	if len(response) == 0 {
		return nil
	}

	result := &ItemsResponse{
		Rows:  make([]*ItemRow, 0, len(response)*len(response[0].Rows)),
		Total: 0,
	}

	index := make(map[keyUnion]int)

	for i := range response {
		r := response[i]

		for j := range r.Rows {
			key := makeKeyUnionMap(r.Rows[j])
			if inx, ok := index[key]; ok {
				unionRowResponse(result.Rows[inx], r.Rows[j])
				continue
			}

			index[key] = len(result.Rows)
			result.Rows = append(result.Rows, r.Rows[j])
		}

		result.Total += r.Total
	}

	return result
}

// UnionValuesResponse union data struct ValuesResponse.
func UnionValuesResponse(response ...*ValuesResponse) *ValuesResponse {
	if len(response) == 0 {
		return nil
	}

	values := make([]*ValueResponse, 0, len(response)*len(response[0].Values))

	index := make(map[keyUnion]int)
	for i := range response {
		r := response[i]
		for j := range r.Values {
			key := makeKeyUnion(r.Values[j])
			if inx, ok := index[key]; ok {
				unionValueResponse(values[inx], r.Values[j])
				continue
			}

			index[key] = len(values)
			values = append(values, r.Values[j])
		}
	}

	return &ValuesResponse{
		Values: values,
	}
}

func makeKeyUnionMap(v *ItemRow) keyUnion {
	return keyUnion(fmt.Sprintf("%v", v.Dimensions))
}

func makeKeyUnion(v *ValueResponse) keyUnion {
	return keyUnion(fmt.Sprintf("%v", v.Key))
}

func unionRowResponse(a, b *ItemRow) {
	if a == nil || b == nil {
		return
	}

	for k := range b.Metrics {
		if _, ok := a.Metrics[k]; !ok {
			a.Metrics[k] = 0
		}
		a.Metrics[k] += b.Metrics[k]
	}
}

func unionValueResponse(a, b *ValueResponse) {
	if a == nil || b == nil {
		return
	}

	a.Count += b.Count
}
