#!/bin/bash

# Configuration
KIBANA_URL="kibana:5601"
EXPORT_FILE="data/exported_objects.ndjson"

# Function to export all Kibana saved objects
export_kibana_data() {
    echo "Exporting all Kibana saved objects..."
    curl -X POST "$KIBANA_URL/api/saved_objects/_export" \
        -H 'kbn-xsrf: true' \
        -H 'Content-Type: application/json' \
        -d'{"type": ["config", "dashboard", "visualization", "search", "index-pattern", "graph-workspace", "map", "canvas-workpad", "canvas-element", "lens", "infrastructure-ui-source", "metrics-explorer-view", "inventory-view", "security-rule"], "excludeExportDetails": true}' \
        -o $EXPORT_FILE

    if [ $? -eq 0 ]; then
        echo "Export successful. Data saved to $EXPORT_FILE."
    else
        echo "Export failed."
        exit 1
    fi
}

# Function to import all Kibana saved objects
import_kibana_data() {
    if [ ! -f "$EXPORT_FILE" ]; then
        echo "Export file $EXPORT_FILE not found. Please export the data first."
        exit 1
    fi

    echo "Importing all Kibana saved objects..."
    curl -X POST "$KIBANA_URL/api/saved_objects/_import" \
        -H 'kbn-xsrf: true' \
        --form file=@$EXPORT_FILE

    if [ $? -eq 0 ]; then
        echo "Import successful."
    else
        echo "Import failed."
        exit 1
    fi
}

# Main script logic
case $1 in
    export)
        export_kibana_data
        ;;
    import)
        import_kibana_data
        ;;
    *)
        echo "Usage: $0 {export|import}"
        exit 1
        ;;
esac
