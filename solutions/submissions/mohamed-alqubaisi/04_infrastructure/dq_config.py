
# Task 4.3 — Configurable Data Quality Rules

DQ_CONFIG = {
    "projects": {
        "completeness_threshold": 0.90,
        "pk_columns": ["project_id"],
        "numeric_ranges": {
            "budget": {"min": 0, "max": 10_000_000},
            "actual_cost": {"min": 0, "max": 10_000_000},
        },
        "date_columns": {
            "start_date": {"allow_future": False},
            "end_date": {"allow_future": True},
        },
        "consistency_rules": [
            {
                "type": "before",
                "columns": ["start_date", "end_date"],
            },
            {
                "type": "non_negative",
                "column": "actual_cost",
            },
        ],
        "foreign_keys": {
            "project_manager_id": (
                "employees", "employee_id"
            ),
        },
    },

    "employees": {
        "completeness_threshold": 0.85,
        "pk_columns": ["employee_id"],
        "numeric_ranges": {
            "salary": {"min": 10000, "max": 100000},
            "years_experience": {"min": 0, "max": 50},
        },
        "date_columns": {
            "hire_date": {"allow_future": False},
        },
        "consistency_rules": [
            {
                "type": "non_negative",
                "column": "salary",
            },
        ],
        "foreign_keys": {
            "manager_id": ("employees", "employee_id"),
        },
    },

    "transactions": {
        "completeness_threshold": 0.80,
        "pk_columns": ["transaction_id"],
        "numeric_ranges": {
            "amount": {"min": 0, "max": 10_000_000},
        },
        "date_columns": {
            "transaction_date": {"allow_future": False},
        },
        "consistency_rules": [
            {
                "type": "non_negative",
                "column": "amount",
            },
        ],
        "foreign_keys": {
            "project_id": ("projects", "project_id"),
            "approved_by": ("employees", "employee_id"),
        },
    },

    "employees_salary_history": {
        "completeness_threshold": 0.85,
        "pk_columns": [
    "employee_id",
    "effective_date",
    "change_type",
],
"conditional_required": {
    "previous_salary": {
        "except_when": {
            "change_type": ["Hire"]
        }
    },
    "previous_role": {
        "except_when": {
            "change_type": ["Hire"]
        }
    },
    "previous_level": {
        "except_when": {
            "change_type": ["Hire"]
        }
    },
},
        "numeric_ranges": {
            "previous_salary": {
                "min": 0, "max": 100000
            },
            "new_salary": {
                "min": 0, "max": 100000
            },
        },
        "date_columns": {
            "effective_date": {"allow_future": False},
        },
        "consistency_rules": [
            {
                "type": "non_negative",
                "column": "new_salary",
            },
        ],
        "foreign_keys": {
            "employee_id": ("employees", "employee_id"),
        },
    },
}
