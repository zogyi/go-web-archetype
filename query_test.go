package go_web_archetype

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestToSql(t *testing.T) {
	jsonString := `{
  "operator": "AND",
  "conditions": [
    {
      "field": "X",
      "value": "5",
      "operator": "gt"
    },
    {
      "operator": "OR",
      "conditions": [
        {
          "field": "Y",
          "value": "%b%",
          "operator": "eq"
        },
        {
          "field": "Z",
          "value": "5",
          "operator": "like"
        }
      ]
    },
    {
      "field": "B",
      "value": "true",
      "operator": "is"
    }
  ]
}`
	test := QueryJSON{}
	err := json.Unmarshal([]byte(jsonString), &test)
	fmt.Println(err)
}
