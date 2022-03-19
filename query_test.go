package go_web_archetype

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestToSql(t *testing.T) {
	jsonString := `	{
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
	        }
	      ]
	    }
	  ]
	}`
	var conn Connector
	fmt.Println(conn)
	test := Query{}
	err := json.Unmarshal([]byte(jsonString), &test)
	fmt.Println(err)
	fmt.Println(test)
	//sqlizer, err := test.ToSQL()
	//fmt.Println(err)
	//fmt.Println(sqlizer.ToSql())
}
