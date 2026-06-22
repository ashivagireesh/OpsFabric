import unittest

from etl_engine import ETLEngine


class RuleIntelligenceEngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = ETLEngine()
        self.rows = [
            {"bc_code": "BC001", "account_number": "A1", "service": "Cash Deposit", "amount": 3000, "transaction_date": "2026-06-01"},
            {"bc_code": "BC001", "account_number": "A1", "service": "Cash Deposit", "amount": 5000, "transaction_date": "2026-06-01"},
            {"bc_code": "BC001", "account_number": "A2", "service": "Cash Deposit", "amount": 1000, "transaction_date": "2026-06-02"},
            {"bc_code": "BC001", "account_number": "A2", "service": "Cash Withdraw", "amount": 900, "transaction_date": "2026-06-02"},
        ]
        self.mapping = {
            "entity_field": "bc_code",
            "service_field": "service",
            "amount_field": "amount",
            "date_field": "transaction_date",
            "account_field": "account_number",
        }

    def test_percentage_cap_monthly_pivot_target_and_anomaly(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_deposit",
                "name": "Cash Deposit Commission",
                "enabled": True,
                "service_value": "Cash Deposit",
                "conditions": [{"field": "amount", "operator": "greater_than", "value": 0}],
                "calculation": {"method": "percentage", "rate_percent": 0.4, "amount_field": "amount"},
                "cap": {"enabled": True, "amount": 25, "group_by": ["account", "day"]},
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "targets": [{
                "id": "target",
                "enabled": True,
                "measure": "count",
                "target_value": 4,
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "anomalies": [{
                "id": "anomaly",
                "enabled": True,
                "measure": "sum",
                "field": "amount",
                "operator": "greater_than",
                "threshold": 8000,
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month"},
        }
        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row["bc_code"], "BC001")
        self.assertEqual(row["period"], "2026-06")
        self.assertEqual(row["cash_deposit_count"], 3)
        self.assertEqual(row["cash_deposit_amount"], 9000)
        self.assertEqual(row["variable_commission"], 29)
        self.assertEqual(row["total_commission"], 29)
        self.assertEqual(row["target_status"], "achieved")
        self.assertEqual(row["anomaly_count"], 1)
        self.assertEqual(row["target_target_status"], "achieved")
        self.assertEqual(row["target_target_actual"], 4)
        self.assertEqual(row["anomaly_anomaly_count"], 1)

    def test_daily_ledger_output(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_deposit",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {"method": "percentage", "rate_percent": 0.4, "amount_field": "amount"},
                "group_by": ["entity", "day", "service"],
                "period_grain": "day",
            }],
            "output_layout": {"mode": "ledger", "row_fields": ["entity", "day"], "period_grain": "day"},
        }
        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 2)
        days = {row["day"] for row in result}
        self.assertEqual(days, {"2026-06-01", "2026-06-02"})

    def test_nested_condition_tree_selects_matching_records(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_services",
                "enabled": True,
                "condition_tree": {
                    "type": "group",
                    "match_mode": "all",
                    "conditions": [
                        {"field": "amount", "operator": "greater_than", "value": 0},
                        {
                            "type": "group",
                            "match_mode": "any",
                            "conditions": [
                                {"field": "service", "operator": "equals", "value": "Cash Deposit"},
                                {"field": "service", "operator": "equals", "value": "Cash Withdraw"},
                            ],
                        },
                    ],
                },
                "calculation": {"method": "percentage", "rate_percent": 1, "amount_field": "amount"},
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["cash_deposit_count"], 3)
        self.assertEqual(result[0]["cash_withdraw_count"], 1)
        self.assertEqual(result[0]["total_service_count"], 4)
        self.assertEqual(result[0]["total_commission"], 99)

    def test_blank_literal_condition_is_treated_as_unfinished(self):
        condition_tree = {
            "type": "group",
            "match_mode": "all",
            "conditions": [{"field": "service", "operator": "equals", "value": ""}],
        }

        self.assertTrue(
            self.engine._rule_engine_matches_conditions(self.rows[0], condition_tree, self.mapping, "all")
        )

        target_rows = self.engine._rule_engine_target_rows(
            self.rows,
            [{
                "id": "target",
                "enabled": True,
                "condition_tree": condition_tree,
                "measures": [{"id": "txn_count", "name": "Txn Count", "measure": "count", "target_value": 4}],
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            self.mapping,
            {"row_fields": ["entity", "period"], "period_grain": "month"},
        )

        self.assertEqual(len(target_rows), 1)
        self.assertEqual(target_rows[0]["actual"], 4)

    def test_selected_output_fields_include_monitoring_columns_on_one_row(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_deposit",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {"method": "percentage", "rate_percent": 0.4, "amount_field": "amount"},
                "cap": {"enabled": True, "amount": 25, "group_by": ["account", "day"]},
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "targets": [{
                "id": "monthly_activity",
                "enabled": True,
                "measures": [{
                    "id": "txn_count_4",
                    "name": "Transaction Count",
                    "measure": "count",
                    "target_value": 4,
                }],
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "anomalies": [{
                "id": "amount_risk",
                "enabled": True,
                "checks": [{
                    "id": "high_amount",
                    "name": "High Amount",
                    "measure": "sum",
                    "field": "amount",
                    "operator": "greater_than",
                    "threshold": 8000,
                    "severity": "warning",
                }],
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "output_layout": {
                "mode": "pivot",
                "row_fields": ["entity", "period"],
                "period_grain": "month",
                "selected_fields": [
                    "bc_code",
                    "period",
                    "total_commission",
                    "target_txn_count_4_status",
                    "target_txn_count_4_actual",
                    "anomaly_high_amount_count",
                    "anomaly_high_amount_severity",
                ],
            },
        }

        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(list(row.keys()), config["output_layout"]["selected_fields"])
        self.assertEqual(row["bc_code"], "BC001")
        self.assertEqual(row["period"], "2026-06")
        self.assertEqual(row["total_commission"], 29)
        self.assertEqual(row["target_txn_count_4_status"], "achieved")
        self.assertEqual(row["target_txn_count_4_actual"], 4)
        self.assertEqual(row["anomaly_high_amount_count"], 1)
        self.assertEqual(row["anomaly_high_amount_severity"], "warning")

    def test_selected_output_fields_resolve_mapped_uppercase_upstream_keys(self):
        rows = [
            {"AGENTCODE": "UBI400001", "SERVICENAME": "TPD DEPOSIT", "AMOUNT": 500, "TXNDATE": "2026-06-01"},
            {"AGENTCODE": "UBI400001", "SERVICENAME": "TPD DEPOSIT", "AMOUNT": 800, "TXNDATE": "2026-06-02"},
        ]
        config = {
            "input_fields": [
                {"id": "agentcode", "role": "entity", "type": "string", "mapped_field": "AGENTCODE"},
                {"id": "servicename", "role": "category", "type": "string", "mapped_field": "SERVICENAME"},
                {"id": "amount", "role": "measure", "type": "number", "mapped_field": "AMOUNT"},
                {"id": "txndate", "role": "date", "type": "date", "mapped_field": "TXNDATE"},
            ],
            "field_mapping": {
                "agentcode": "AGENTCODE",
                "servicename": "SERVICENAME",
                "amount": "AMOUNT",
                "txndate": "TXNDATE",
                "entity_field": "AGENTCODE",
                "service_field": "SERVICENAME",
                "amount_field": "AMOUNT",
                "date_field": "TXNDATE",
            },
            "rules": [{
                "id": "tpd_deposit",
                "enabled": True,
                "conditions": [{"field": "servicename", "operator": "equals", "value": "TPD DEPOSIT"}],
                "calculation": {"method": "percentage", "rate_percent": 0.5, "amount_field": "amount"},
                "group_by": ["AGENTCODE", "period", "SERVICENAME"],
                "period_grain": "month",
            }],
            "output_layout": {
                "mode": "pivot",
                "row_fields": ["AGENTCODE", "period"],
                "period_grain": "month",
                "selected_fields": ["agentcode", "period", "tpd_deposit_count", "tpd_deposit_amount", "total_commission"],
            },
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(list(row.keys()), config["output_layout"]["selected_fields"])
        self.assertEqual(row["agentcode"], "UBI400001")
        self.assertEqual(row["period"], "2026-06")
        self.assertEqual(row["tpd_deposit_count"], 2)
        self.assertEqual(row["tpd_deposit_amount"], 1300)

    def test_group_filter_removes_aggregated_groups_after_row_filter(self):
        rows = [
            {"bc_code": "BC001", "account_number": "A1", "service": "Cash Deposit", "amount": 100, "transaction_date": "2026-06-01", "transaction_id": "T1"},
            {"bc_code": "BC001", "account_number": "A2", "service": "Cash Deposit", "amount": 100, "transaction_date": "2026-06-01", "transaction_id": "T1"},
            {"bc_code": "BC001", "account_number": "A3", "service": "Cash Deposit", "amount": 90, "transaction_date": "2026-06-01", "transaction_id": "T2"},
        ]
        mapping = {
            **self.mapping,
            "transaction_id": "transaction_id",
        }
        base_rule = {
            "id": "cash_deposit",
            "enabled": True,
            "conditions": [
                {"field": "service", "operator": "equals", "value": "Cash Deposit"},
                {"field": "amount", "operator": "less_or_equal", "value": 100},
            ],
            "calculation": {"method": "percentage", "rate_percent": 1, "amount_field": "amount"},
            "group_by": ["transaction_id"],
            "period_grain": "month",
        }
        without_group_filter = {
            "field_mapping": mapping,
            "rules": [base_rule],
            "output_layout": {"mode": "pivot", "row_fields": ["transaction_id"], "period_grain": "month"},
        }
        with_group_filter = {
            "field_mapping": mapping,
            "rules": [{**base_rule, "group_filter": {"enabled": True, "metric": "service_amount", "operator": "less_or_equal", "value": 100}}],
            "output_layout": {"mode": "pivot", "row_fields": ["transaction_id"], "period_grain": "month"},
        }

        unfiltered = self.engine._transform_rule_intelligence(rows, without_group_filter, execution_context={"node_warnings": []})
        filtered = self.engine._transform_rule_intelligence(rows, with_group_filter, execution_context={"node_warnings": []})

        self.assertEqual({row["transaction_id"]: row["cash_deposit_amount"] for row in unfiltered}, {"T1": 200, "T2": 90})
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["transaction_id"], "T2")
        self.assertEqual(filtered[0]["cash_deposit_amount"], 90)

    def test_distinct_day_metric_counts_calendar_days_from_timestamps(self):
        rows = [
            {"bc_code": "BC001", "service": "Login", "amount": 0, "transaction_date": "2026-06-01 09:00:00"},
            {"bc_code": "BC001", "service": "Login", "amount": 0, "transaction_date": "2026-06-01 17:00:00"},
            {"bc_code": "BC001", "service": "Login", "amount": 0, "transaction_date": "2026-06-02 10:00:00"},
        ]

        self.assertEqual(
            self.engine._rule_engine_metric_value(rows, "distinct_count", "date", self.mapping),
            3,
        )
        self.assertEqual(
            self.engine._rule_engine_metric_value(rows, "distinct_day_count", "date", self.mapping),
            2,
        )

    def test_multiple_aggregate_group_filters_gate_flat_commission_once_per_group(self):
        rows = [
            {"bc_code": "BC001", "service": "Enrollment", "amount": 10, "transaction_date": "2026-06-01 09:00:00", "enrollment_count": 1},
            {"bc_code": "BC001", "service": "Enrollment", "amount": 20, "transaction_date": "2026-06-01 10:00:00", "enrollment_count": 0},
            {"bc_code": "BC001", "service": "Enrollment", "amount": 30, "transaction_date": "2026-06-02 09:00:00", "enrollment_count": 1},
            {"bc_code": "BC002", "service": "Enrollment", "amount": 40, "transaction_date": "2026-06-01 09:00:00", "enrollment_count": 1},
            {"bc_code": "BC002", "service": "Enrollment", "amount": 50, "transaction_date": "2026-06-01 10:00:00", "enrollment_count": 1},
        ]
        config = {
            "field_mapping": {**self.mapping, "enrollment_count": "enrollment_count"},
            "rules": [{
                "id": "active_bc_fixed",
                "enabled": True,
                "service_value": "Enrollment",
                "calculation": {"method": "flat", "amount": 100, "per_row": False, "commission_type": "fixed"},
                "commission_type": "fixed",
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
                "group_filters": [
                    {"enabled": True, "metric": "distinct_day_count", "field": "date", "operator": "greater_or_equal", "value": 2},
                    {"enabled": True, "metric": "count", "operator": "greater_or_equal", "value": 3},
                    {"enabled": True, "metric": "sum", "field": "enrollment_count", "operator": "greater_or_equal", "value": 2},
                ],
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["bc_code"], "BC001")
        self.assertEqual(result[0]["fixed_commission"], 100)
        self.assertEqual(result[0]["total_service_count"], 3)

    def test_settlement_output_consolidates_multiple_rule_components(self):
        rows = [
            {"bc_code": "BC001", "service": "Cash Deposit", "amount": 1000, "transaction_date": "2026-06-01", "transaction_id": "T1"},
            {"bc_code": "BC001", "service": "Cash Deposit", "amount": 1500, "transaction_date": "2026-06-02", "transaction_id": "T2"},
            {"bc_code": "BC001", "service": "Mini Statement", "amount": 0, "transaction_date": "2026-06-03", "transaction_id": "T3"},
        ]
        config = {
            "field_mapping": {
                **self.mapping,
                "transaction_id": "transaction_id",
            },
            "rules": [
                {
                    "id": "bc_fixed_charge",
                    "name": "BC Fixed Charge",
                    "enabled": True,
                    "calculation": {"method": "flat", "amount": 100, "per_row": False, "commission_type": "fixed"},
                    "commission_type": "fixed",
                    "group_by": ["entity", "period"],
                    "period_grain": "month",
                    "group_filters": [
                        {"enabled": True, "metric": "distinct_count", "field": "transaction_id", "operator": "greater_or_equal", "value": 3},
                    ],
                },
                {
                    "id": "cash_deposit",
                    "name": "Cash Deposit Commission",
                    "enabled": True,
                    "conditions": [{"field": "service", "operator": "equals", "value": "Cash Deposit"}],
                    "calculation": {"method": "percentage", "rate_percent": 1, "amount_field": "amount", "commission_type": "variable"},
                    "commission_type": "variable",
                    "group_by": ["entity", "period"],
                    "period_grain": "month",
                },
            ],
            "output_layout": {
                "mode": "settlement",
                "row_fields": ["entity", "period"],
                "period_grain": "month",
                "include_audit": True,
            },
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row["record_type"], "settlement")
        self.assertEqual(row["bc_code"], "BC001")
        self.assertEqual(row["period"], "2026-06")
        self.assertEqual(row["bc_fixed_charge_commission"], 100)
        self.assertEqual(row["cash_deposit_commission"], 25)
        self.assertEqual(row["fixed_commission"], 100)
        self.assertEqual(row["variable_commission"], 25)
        self.assertEqual(row["total_commission"], 125)
        self.assertEqual(row["component_count"], 2)
        self.assertEqual(len(row["_audit_traces"]), 2)

    def test_payout_offset_months_shifts_settlement_period_and_keeps_earning_period(self):
        rows = [
            {"bc_code": "BC001", "service": "Account Opening", "amount": 0, "transaction_date": "2026-01-15"},
        ]
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "account_opening_m3",
                "enabled": True,
                "service_value": "Account Opening",
                "calculation": {
                    "method": "flat",
                    "amount": 10,
                    "per_row": True,
                    "commission_type": "fixed",
                    "payout_offset_months": 2,
                },
                "commission_type": "fixed",
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "settlement", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["period"], "2026-03")
        self.assertEqual(result[0]["earning_period"], "2026-01")
        self.assertEqual(result[0]["payout_period"], "2026-03")
        self.assertEqual(result[0]["account_opening_m3_commission"], 10)

    def test_payout_stages_support_funded_ratio_and_per_account_balance_slabs(self):
        rows = [
            {"bc_code": "BC001", "account_number": "A1", "service": "Account Opening", "open_date": "2026-01-05", "balance_date": "2026-02-01", "avg_balance": 250},
            {"bc_code": "BC001", "account_number": "A2", "service": "Account Opening", "open_date": "2026-01-06", "balance_date": "2026-02-01", "avg_balance": 750},
            {"bc_code": "BC001", "account_number": "A3", "service": "Account Opening", "open_date": "2026-01-07", "balance_date": "2026-02-01", "avg_balance": 2500},
        ]
        config = {
            "field_mapping": {
                **self.mapping,
                "date_field": "open_date",
                "account_field": "account_number",
                "avg_balance": "avg_balance",
                "balance_date": "balance_date",
                "open_date": "open_date",
            },
            "rules": [{
                "id": "ntb_account_opening",
                "enabled": True,
                "service_value": "Account Opening",
                "commission_type": "fixed",
                "group_by": ["entity", "period"],
                "period_grain": "month",
                "calculation": {
                    "method": "flat",
                    "amount": 10,
                    "per_distinct": True,
                    "distinct_by": "account_number",
                    "commission_type": "fixed",
                    "stages": [
                        {"id": "opening_charge", "component_id": "opening_charge", "name": "Opening Charge"},
                        {
                            "id": "m3_balance_payout",
                            "component_id": "m3_balance_payout",
                            "name": "M3 Balance Payout",
                            "method": "slab",
                            "basis": "average_balance",
                            "basis_metric": "avg",
                            "basis_field": "avg_balance",
                            "date_field": "balance_date",
                            "anchor_field": "open_date",
                            "window_start_months": 1,
                            "window_end_months": 1,
                            "apply": "per_distinct",
                            "distinct_by": "account_number",
                            "payout_offset_months": 2,
                            "group_filters": [
                                {"metric": "funded_ratio", "field": "avg_balance", "account_field": "account_number", "date_field": "balance_date", "anchor_field": "open_date", "window_start_months": 1, "window_end_months": 1, "funded_threshold": 100, "operator": "greater_or_equal", "value": 90},
                                {"metric": "distinct_count", "field": "account_number", "operator": "greater_or_equal", "value": 3},
                            ],
                            "slabs": [
                                {"min": 0, "max": 100, "method": "flat", "amount": 0},
                                {"min": 101, "max": 500, "method": "flat", "amount": 10},
                                {"min": 501, "max": 2000, "method": "flat", "amount": 30},
                                {"min": 2001, "method": "flat", "amount": 50},
                            ],
                        },
                    ],
                },
            }],
            "output_layout": {"mode": "settlement", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        by_period = {row["period"]: row for row in result}
        self.assertEqual(set(by_period), {"2026-01", "2026-03"})
        self.assertEqual(by_period["2026-01"]["opening_charge_commission"], 30)
        self.assertEqual(by_period["2026-03"]["earning_period"], "2026-01")
        self.assertEqual(by_period["2026-03"]["m3_balance_payout_commission"], 90)

    def test_commission_shares_expand_one_rule_into_named_recipient_components(self):
        rows = [
            {"bc_code": "BC001", "service": "Loan Lead", "amount": 1000, "transaction_date": "2026-06-01"},
        ]
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "loan_lead",
                "enabled": True,
                "service_value": "Loan Lead",
                "component_id": "loan_lead",
                "calculation": {
                    "method": "percentage",
                    "rate_percent": 10,
                    "amount_field": "amount",
                    "shares": [
                        {"id": "originator", "component_id": "loan_lead_originator_share", "payee_type": "Originator", "percent": 80},
                        {"id": "partner", "component_id": "loan_lead_partner_share", "payee_type": "Partner", "percent": 20},
                    ],
                },
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "settlement", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        row = result[0]
        self.assertEqual(row["loan_lead_originator_share_commission"], 80)
        self.assertEqual(row["loan_lead_partner_share_commission"], 20)
        self.assertEqual(row["total_commission"], 100)
        self.assertEqual(row["component_count"], 2)

    def test_cap_scope_allocates_back_to_report_groups(self):
        rows = [
            {"bc_code": "BC001", "account_number": "A1", "service": "Cash Deposit", "amount": 100, "transaction_date": "2026-06-01"},
            {"bc_code": "BC002", "account_number": "A1", "service": "Cash Deposit", "amount": 200, "transaction_date": "2026-06-01"},
        ]
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_deposit",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {"method": "percentage", "rate_percent": 10, "amount_field": "amount"},
                "cap": {"enabled": True, "amount": 15, "group_by": ["account", "day"]},
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        by_bc = {row["bc_code"]: row for row in result}
        self.assertEqual(set(by_bc), {"BC001", "BC002"})
        self.assertEqual(by_bc["BC001"]["cash_deposit_count"], 1)
        self.assertEqual(by_bc["BC002"]["cash_deposit_count"], 1)
        self.assertEqual(by_bc["BC001"]["total_commission"], 5)
        self.assertEqual(by_bc["BC002"]["total_commission"], 10)

    def test_monitoring_rows_do_not_create_incomplete_detail_pivots(self):
        rows = [
            {"bc_code": "BC001", "account_number": "A1", "service": "Cash Deposit", "amount": 500, "transaction_date": "2026-06-01", "transaction_id": "T1"},
            {"bc_code": "BC001", "account_number": "A2", "service": "Cash Deposit", "amount": 25000, "transaction_date": "2026-06-01", "transaction_id": "T2"},
        ]
        config = {
            "field_mapping": {
                **self.mapping,
                "transaction_id": "transaction_id",
            },
            "rules": [{
                "id": "cash_deposit",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {"method": "percentage", "rate_percent": 1, "amount_field": "amount"},
                "group_by": ["transaction_id", "entity", "day", "service"],
                "period_grain": "day",
            }],
            "targets": [{
                "id": "target",
                "enabled": True,
                "measure": "count",
                "target_value": 1,
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "anomalies": [{
                "id": "anomaly",
                "enabled": True,
                "measure": "sum",
                "field": "amount",
                "operator": "greater_than",
                "threshold": 1000,
                "group_by": ["entity", "period"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["transaction_id", "entity", "day"], "period_grain": "day"},
        }

        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 2)
        self.assertEqual({row["transaction_id"] for row in result}, {"T1", "T2"})
        self.assertFalse(any(row.get("transaction_id") in {None, ""} for row in result))

    def test_target_and_anomaly_nested_conditions_filter_before_grouping(self):
        config = {
            "field_mapping": self.mapping,
            "targets": [{
                "id": "cash_deposit_target",
                "enabled": True,
                "measure": "sum",
                "field": "amount",
                "target_value": 8000,
                "group_by": ["entity", "period"],
                "period_grain": "month",
                "condition_tree": {
                    "type": "group",
                    "match_mode": "all",
                    "conditions": [
                        {"field": "amount", "operator": "greater_than", "value": 0},
                        {
                            "type": "group",
                            "match_mode": "any",
                            "conditions": [
                                {"field": "service", "operator": "equals", "value": "Cash Deposit"},
                                {"field": "service", "operator": "equals", "value": "Mini Statement"},
                            ],
                        },
                    ],
                },
            }],
            "anomalies": [{
                "id": "withdrawal_anomaly",
                "enabled": True,
                "measure": "sum",
                "field": "amount",
                "operator": "greater_than",
                "threshold": 800,
                "group_by": ["entity", "period"],
                "period_grain": "month",
                "condition_tree": {
                    "type": "group",
                    "match_mode": "all",
                    "conditions": [
                        {"field": "amount", "operator": "greater_than", "value": 0},
                        {
                            "type": "group",
                            "match_mode": "any",
                            "conditions": [
                                {"field": "service", "operator": "equals", "value": "Cash Withdraw"},
                                {"field": "service", "operator": "equals", "value": "Balance Enquiry"},
                            ],
                        },
                    ],
                },
            }],
            "output_layout": {"mode": "all", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        target_rows = [row for row in result if row.get("record_type") == "target"]
        anomaly_rows = [row for row in result if row.get("record_type") == "anomaly"]
        self.assertEqual(len(target_rows), 1)
        self.assertEqual(target_rows[0]["actual"], 9000)
        self.assertEqual(target_rows[0]["target_status"], "achieved")
        self.assertEqual(len(anomaly_rows), 1)
        self.assertEqual(anomaly_rows[0]["actual"], 900)
        self.assertEqual(anomaly_rows[0]["anomaly_count"], 1)

    def test_target_and_anomaly_support_multiple_measures(self):
        config = {
            "field_mapping": self.mapping,
            "targets": [{
                "id": "monthly_performance",
                "name": "Monthly Performance",
                "enabled": True,
                "group_by": ["entity", "period"],
                "period_grain": "month",
                "measures": [
                    {"id": "txn_count", "name": "Transaction Count", "measure": "count", "target_value": 4, "warning_percent": 80},
                    {"id": "total_amount", "name": "Total Amount", "measure": "sum", "field": "amount", "target_value": 10000, "warning_percent": 80},
                ],
            }],
            "anomalies": [{
                "id": "risk_checks",
                "name": "Risk Checks",
                "enabled": True,
                "group_by": ["entity", "period"],
                "period_grain": "month",
                "checks": [
                    {"id": "high_amount", "name": "High Amount", "measure": "sum", "field": "amount", "operator": "greater_than", "threshold": 9000, "severity": "warning"},
                    {"id": "high_count", "name": "High Count", "measure": "count", "operator": "greater_than", "threshold": 10, "severity": "critical"},
                ],
            }],
            "output_layout": {"mode": "all", "row_fields": ["entity", "period"], "period_grain": "month"},
        }

        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        target_rows = [row for row in result if row.get("record_type") == "target"]
        anomaly_rows = [row for row in result if row.get("record_type") == "anomaly"]
        self.assertEqual({row["target_measure_id"] for row in target_rows}, {"txn_count", "total_amount"})
        self.assertEqual({row["target_status"] for row in target_rows}, {"achieved", "at_risk"})
        self.assertEqual(len(anomaly_rows), 1)
        self.assertEqual(anomaly_rows[0]["anomaly_check_id"], "high_amount")
        self.assertEqual(anomaly_rows[0]["anomaly_measure"], "sum")

    def test_slab_rule(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "slab",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {
                    "method": "slab",
                    "basis": "amount",
                    "slabs": [
                        {"min": 0, "max": 5000, "method": "flat", "amount": 10},
                        {"min": 5001, "method": "percentage", "rate_percent": 1},
                    ],
                },
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month"},
        }
        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        self.assertEqual(result[0]["cash_deposit_commission"], 90)
        self.assertEqual(result[0]["total_commission"], 90)

    def test_output_can_disable_audit_payload(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_deposit",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {"method": "percentage", "rate_percent": 0.4, "amount_field": "amount"},
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month", "include_audit": False},
        }
        result = self.engine._transform_rule_intelligence(self.rows, config, execution_context={"node_warnings": []})

        self.assertNotIn("_audit_traces", result[0])

    def test_generic_logical_fields_mapping(self):
        rows = [
            {"branch": "B001", "txn_date": "2026-06-01", "total": 1000, "product_type": "A", "reference": "R1"},
            {"branch": "B001", "txn_date": "2026-06-01", "total": 2000, "product_type": "A", "reference": "R1"},
            {"branch": "B001", "txn_date": "2026-06-02", "total": 500, "product_type": "B", "reference": "R2"},
        ]
        config = {
            "input_fields": [
                {"id": "entity_id", "label": "Entity", "type": "string", "role": "entity", "required": True},
                {"id": "event_date", "label": "Date", "type": "date", "role": "date", "required": True},
                {"id": "measure_value", "label": "Measure", "type": "number", "role": "measure"},
                {"id": "category", "label": "Category", "type": "string", "role": "category"},
                {"id": "scope_id", "label": "Scope", "type": "string", "role": "identifier"},
            ],
            "field_mapping": {
                "entity_id": "branch",
                "event_date": "txn_date",
                "measure_value": "total",
                "category": "product_type",
                "scope_id": "reference",
            },
            "rules": [{
                "id": "generic_rule",
                "enabled": True,
                "conditions": [
                    {"field": "category", "operator": "equals", "value": "A"},
                    {"field": "measure_value", "operator": "greater_than", "value": 0},
                ],
                "calculation": {"method": "percentage", "rate_percent": 1, "amount_field": "measure_value"},
                "cap": {"enabled": True, "amount": 25, "group_by": ["scope_id", "event_date"]},
                "group_by": ["entity_id", "period", "category"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity_id", "period"], "period_grain": "month"},
        }
        result = self.engine._transform_rule_intelligence(rows, config, execution_context={"node_warnings": []})

        self.assertEqual(result[0]["entity_id"], "B001")
        self.assertEqual(result[0]["period"], "2026-06")
        self.assertEqual(result[0]["a_count"], 2)
        self.assertEqual(result[0]["a_amount"], 3000)
        self.assertEqual(result[0]["total_commission"], 25)

    def test_upstream_wrapper_payload_is_flattened(self):
        config = {
            "field_mapping": self.mapping,
            "rules": [{
                "id": "cash_deposit",
                "enabled": True,
                "service_value": "Cash Deposit",
                "calculation": {"method": "percentage", "rate_percent": 0.4, "amount_field": "amount"},
                "group_by": ["entity", "period", "service"],
                "period_grain": "month",
            }],
            "output_layout": {"mode": "pivot", "row_fields": ["entity", "period"], "period_grain": "month"},
        }
        wrapped_rows = [{
            "rows": self.rows,
            "columns": ["bc_code", "account_number", "service", "amount", "transaction_date"],
            "row_count": len(self.rows),
        }]

        result = self.engine._transform_rule_intelligence(wrapped_rows, config, execution_context={"node_warnings": []})

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["cash_deposit_count"], 3)
        self.assertEqual(result[0]["cash_deposit_amount"], 9000)


if __name__ == "__main__":
    unittest.main()
