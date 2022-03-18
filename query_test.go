package go_web_archetype

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestToSql(t *testing.T) {
	jsonString := `{
  "connector": "AND",
  "conditions": [
    {
      "field": "X",
      "value": "5",
      "operator": "gt"
    },
    {
      "connector": "OR",
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
	fmt.Println(test)
	fmt.Println(test.ToSQL())
}
