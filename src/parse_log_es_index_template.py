

DS_STATICS_TEMPLATE = {

    "template": "datastage-summary-run-*",
    "settings": {
      "index": {
        "refresh_interval": "60s"
      }
    },
    "mappings": {
      "_default_": {
        "dynamic_templates": [
          {
            "strings": {
              "mapping": {
                "type": "keyword"
              },
              "match_mapping_type": "string",
              "match": "*"
            }
          }
        ],
        "properties": {
          "start_eid": {
            "type": "integer"
          },
          "end_eid": {
            "type": "integer"
          },
          "duration": {
            "type": "text"
          },
          "message": {
            "type": "text"
          }
        },
        "_all": {
          "enabled": "false"
        }
      }
    },
    "aliases": {}
    }
