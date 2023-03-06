user_profiles_jsonl = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "email",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "full_name",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "state",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "birth_date",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "phone_number",
                "type": "STRING",
                "mode": "NULLABLE"
            },
        ]
    },
    "sourceFormat": "NEWLINE_DELIMITED_JSON",
    "sourceUris": [
        (
            "gs://{{ params.data_lake_raw_bucket }}"
            "/user_profiles"
            "/user_profiles.jsonl"
        )
    ]
}
