# Agent Portfolio Summary API Specification - REST + GraphQL

## Index
| Section | Topic |
|---|---|
| 1 | Introduction and purpose |
| 2 | Document index |
| 3 | Glossary |
| 4 | API surface |
| 5 | Complete API and module-wise APIs |
| 6 | Modules returned |
| 7 | Time windows |
| 8 | Section codes |
| 9 | Module-wise required highlights |
| 10 | Complete request and response schemas |
| 11 | Module-wise request and response schemas |
| 12 | Standard response shape |
| 13 | REST API specification |
| 14 | REST request and response examples |
| 15 | GraphQL API, query, variables, and response examples |
| 16 | Field catalog |
| 17 | Error contract |
| 18 | Frontend guidance |

## Glossary
| Term | Meaning |
|---|---|
| Agent / BC | Business Correspondent or agent whose portfolio is retrieved. |
| Portfolio Summary | Consolidated agent view across transactions, commission, system health, targets, risk, audits, analytics, and EAPRM. |
| Window Code | Named time range such as LAST_7_DAYS, LAST_3_MONTHS, or CURRENT_FY_QUARTERS. |
| Grain | Trend period type: DAY, MONTH, or QUARTER. |
| Section Code | API code identifying one module, such as TRANSACTION_SUMMARY. |
| Overall | Summary values used by dashboard cards. |
| Trend Summary | Array of period rows used by charts and drill-down. |
| Breakdowns | Channel, transaction type, service type, product, or category splits. |
| Narrative | Simple business explanation returned with KPI values. |
| EAPRM | Enterprise Agent Performance Risk Management score and component score module. |
| iScore | Enterprise score from EAPRM used to classify agent risk/performance. |
| BD / TD | Business Decline and Technical Decline response code categories. |

## Module-wise APIs
| Module | Section Code | REST API |
|---|---|---|
| Transaction Summary | `TRANSACTION_SUMMARY` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/TRANSACTION_SUMMARY` |
| Commission Summary | `COMMISSION_SUMMARY` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/COMMISSION_SUMMARY` |
| System Performance | `SYSTEM_PERFORMANCE` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/SYSTEM_PERFORMANCE` |
| Target & Campaign Management | `TARGET_CAMPAIGN_MANAGEMENT` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/TARGET_CAMPAIGN_MANAGEMENT` |
| Anomaly Risk Management | `ANOMALY_RISK_MANAGEMENT` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/ANOMALY_RISK_MANAGEMENT` |
| Agent Audit Management | `AGENT_AUDIT_MANAGEMENT` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/AGENT_AUDIT_MANAGEMENT` |
| Analytics Reporting Management | `ANALYTICS_REPORTING_MANAGEMENT` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/ANALYTICS_REPORTING_MANAGEMENT` |
| Enterprise Agent Performance Risk Management | `ENTERPRISE_AGENT_PERFORMANCE_RISK_MANAGEMENT` | `GET /sbos-ibpm/v1/agents/{agentCode}/portfolio-summary/sections/ENTERPRISE_AGENT_PERFORMANCE_RISK_MANAGEMENT` |

## Module-wise Required Highlights
| Module | Last 7 Days - Daily | Last 3 Months - Monthly | Current FY 4 Quarters - Quarterly |
|---|---|---|---|
| Transaction Summary | Daily total transactions, financial/non-financial split, amount involved, cash inflow/outflow, top channel, top transaction type, service mix. | Month-wise transaction count, amount involved, financial transaction ratio, cash movement, enrollment/lead/ease service movement. | Quarter-wise transaction growth, amount growth, channel mix, financial transaction ratio trend, service adoption trend. |
| Commission Summary | Daily commission earned, eligible transaction count, eligible amount, average commission, rank movement. | Month-wise commission earned, eligible transaction value, average commission, rank trend. | Quarter-wise commission growth, rank movement, commission productivity, eligible transaction quality. |
| System Performance | Daily success ratio, failure ratio, BD count, TD count, top BD/TD response code. | Month-wise success/failure trend, dominant decline type, top recurring response code. | Quarter-wise system health band, error movement, operational stability comparison. |
| Target & Campaign Management | Daily target achievement, pending transaction count, required transactions per day, target status. | Month-wise targets assigned, achieved, pending, average achievement percent. | Quarter-wise target completion, campaign effectiveness, missed target concentration, achievement band. |
| Anomaly Risk Management | Daily suspicious transaction count/ratio, suspicious amount, suspicious customers, top anomaly case. | Month-wise suspicious amount, anomaly case count, repeated customers, risk band movement. | Quarter-wise risk trend, fraud/anomaly concentration, high-risk pattern movement. |
| Agent Audit Management | Daily audits initiated/closed, ongoing audits, audit marks when available, latest audit status. | Month-wise audit volume, average marks, ongoing audit backlog, audit source split. | Quarter-wise audit quality, recurring audit source, compliance improvement, critical audit count. |
| Analytics Reporting Management | Daily recommendations by type/severity, open/accepted/closed recommendations, top module. | Month-wise recommendation volume, major/critical recommendations, acceptance and closure trend. | Quarter-wise recommendation effectiveness, unresolved recommendation load, priority trend. |
| Enterprise Agent Performance Risk Management | Daily iScore, band, operational/compliance/financial health scores, attendance, customers engaged. | Month-wise iScore movement, component score trend, attendance trend, customer engagement trend. | Quarter-wise iScore band movement, component score improvement, risk-performance trajectory. |

## Complete Request Schema
| Field | Type | Required | Example | Meaning |
|---|---|---|---|---|
| `agentCode` | path string | Yes | BC00012345 | Agent/BC code in URL path. |
| `windowCodes[]` | array enum | No | LAST_7_DAYS, LAST_3_MONTHS, CURRENT_FY_QUARTERS | Time windows to return. |
| `sectionCodes[]` | array enum | No | TRANSACTION_SUMMARY, COMMISSION_SUMMARY | Modules to include. Omit for all modules. |
| `includeTrends` | boolean | No | true | Return trendSummary.periods[]. |
| `includeBreakdowns` | boolean | No | true | Return breakdowns object. |
| `includeNarratives` | boolean | No | true | Return narrative text. |
| `filters.channelCodes[]` | array string | No | ONUS, OFFUS | Filter by channel. |
| `filters.transactionTypeCodes[]` | array string | No | AEPS, RUPAY | Filter by transaction type. |
| `filters.serviceTypeCodes[]` | array string | No | CD, CW, FT | Filter by service type. |
| `filters.enrollmentTypeCodes[]` | array string | No | PMJJBY, PMSBY, APY | Filter by enrollment product. |
| `filters.leadServiceTypeCodes[]` | array string | No | Loan, RD, FD | Filter by lead service. |
| `filters.easeServiceTypeCodes[]` | array string | No | Debit Card Hotlisting | Filter by ease service. |

## Complete Response Schema
| Field | Type | Required | Meaning |
|---|---|---|---|
| `requestId` | string | Yes | Correlation id for support and logs. |
| `schemaVersion` | string | Yes | API schema version. |
| `referenceDataVersion` | string | Yes | Reference taxonomy version. |
| `generatedAt` | datetime | Yes | Response timestamp. |
| `agent` | object | Yes | Agent identity and hierarchy. |
| `portfolioStatus` | object | Yes | Overall band, refresh state, data freshness. |
| `timeWindows[]` | array | Yes | One object per requested window. |
| `timeWindows[].sections` | object | Yes | Map of module JSON sections. |
| `recommendations[]` | array | No | Cross-module recommendations. |

## Module Request Schema
| Field | Type | Required | Meaning |
|---|---|---|---|
| `agentCode` | path string | Yes | Agent/BC code. |
| `sectionCode` | path enum | Yes | One value from Section Codes table. |
| `windowCodes` | query CSV | No | LAST_7_DAYS,LAST_3_MONTHS,CURRENT_FY_QUARTERS. |
| `includeTrends` | query boolean | No | Include day/month/quarter trend rows. |
| `includeBreakdowns` | query boolean | No | Include split by channel/type/service where applicable. |
| `includeNarratives` | query boolean | No | Include business explanation. |

## Module Response Schema
| Field | Type | Required | Meaning |
|---|---|---|---|
| `requestId` | string | Yes | Correlation id. |
| `generatedAt` | datetime | Yes | Response timestamp. |
| `agent` | object | Yes | Agent identity. |
| `sectionCode` | enum | Yes | Requested module code. |
| `timeWindows[]` | array | Yes | Window rows for requested module only. |
| `timeWindows[].section.overall` | object | Yes | Module dashboard summary. |
| `timeWindows[].section.trendSummary.periods[]` | array | If requested | Day/month/quarter trend rows. |
| `timeWindows[].section.breakdowns` | object | If requested | Module-specific breakdowns. |
| `timeWindows[].section.narrative` | string | If requested | Module explanation. |

## REST Advanced Query Request
```json
{
  "windowCodes": [
    "LAST_7_DAYS",
    "LAST_3_MONTHS",
    "CURRENT_FY_QUARTERS"
  ],
  "sectionCodes": [
    "TRANSACTION_SUMMARY",
    "COMMISSION_SUMMARY",
    "SYSTEM_PERFORMANCE",
    "TARGET_CAMPAIGN_MANAGEMENT",
    "ANOMALY_RISK_MANAGEMENT",
    "AGENT_AUDIT_MANAGEMENT",
    "ANALYTICS_REPORTING_MANAGEMENT",
    "ENTERPRISE_AGENT_PERFORMANCE_RISK_MANAGEMENT"
  ],
  "includeTrends": true,
  "includeBreakdowns": true,
  "includeNarratives": true,
  "filters": {
    "channelCodes": [
      "ONUS",
      "OFFUS"
    ],
    "transactionTypeCodes": [
      "AEPS",
      "RUPAY",
      "SHG",
      "TPD",
      "IMPS"
    ],
    "serviceTypeCodes": [
      "CD",
      "CW",
      "FT"
    ],
    "enrollmentTypeCodes": [
      "PMJJBY",
      "PMSBY",
      "APY"
    ],
    "leadServiceTypeCodes": [
      "Loan",
      "RD",
      "FD"
    ],
    "easeServiceTypeCodes": [
      "Debit Card Hotlisting",
      "Account Enquiry"
    ]
  }
}
```

## REST Filled Response
```json
{
  "requestId": "req_01HY9Z6N0W5D4Z7D6T7E8K9P2A",
  "schemaVersion": "2.0",
  "referenceDataVersion": "svc-taxonomy-2026-05",
  "generatedAt": "2026-05-20T09:30:00+05:30",
  "agent": {
    "agentCode": "BC00012345",
    "displayName": "Sample BC Agent",
    "status": "ACTIVE",
    "branchCode": "BR001",
    "regionCode": "RG-SOUTH",
    "hierarchyLevel": "AGENT"
  },
  "portfolioStatus": {
    "overallBand": "Stable",
    "dailyRefreshStatus": "COMPLETED",
    "dataFreshness": {
      "latestTxnDate": "2026-05-19",
      "latestCommissionDate": "2026-05-18",
      "latencyMinutes": 18
    },
    "warning": null
  },
  "timeWindows": [
    {
      "code": "LAST_7_DAYS",
      "label": "Last 7 Days",
      "fromDate": "2026-05-13",
      "toDate": "2026-05-19",
      "grain": "DAY",
      "periodStatus": "COMPLETED",
      "sections": {
        "transactionSummary": {
          "overall": {
            "financialTxnCount": 184,
            "nonFinancialTxnCount": 59,
            "enrollmentCount": 12,
            "leadServiceCount": 8,
            "easeServiceCount": 19,
            "totalTxnCount": 243,
            "amountInvolved": {
              "value": 1854000.0,
              "currency": "INR"
            },
            "financialTxnRatio": 75.72,
            "averageFinancialTxnRatio": 72.16,
            "cashInflow": {
              "value": 984000.0,
              "currency": "INR"
            },
            "cashOutflow": {
              "value": 870000.0,
              "currency": "INR"
            }
          },
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "periodLabel": "13 May 2026",
                "grain": "DAY",
                "fromDate": "2026-05-13",
                "toDate": "2026-05-13",
                "financialTxnCount": 22,
                "nonFinancialTxnCount": 9,
                "totalTxnCount": 31,
                "amountInvolved": {
                  "value": 212000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "2026-05-14",
                "periodLabel": "14 May 2026",
                "grain": "DAY",
                "fromDate": "2026-05-14",
                "toDate": "2026-05-14",
                "financialTxnCount": 25,
                "nonFinancialTxnCount": 9,
                "totalTxnCount": 34,
                "amountInvolved": {
                  "value": 244000.0,
                  "currency": "INR"
                }
              }
            ]
          },
          "breakdowns": {
            "byChannel": [
              {
                "code": "ONUS",
                "totalTxnCount": 138,
                "amountInvolved": 1042000.0
              },
              {
                "code": "OFFUS",
                "totalTxnCount": 105,
                "amountInvolved": 812000.0
              }
            ],
            "byTransactionType": [
              {
                "code": "AEPS",
                "totalTxnCount": 121,
                "amountInvolved": 1021000.0
              },
              {
                "code": "RUPAY",
                "totalTxnCount": 44,
                "amountInvolved": 276000.0
              }
            ],
            "byServiceType": [
              {
                "code": "CD",
                "totalTxnCount": 74,
                "amountInvolved": 984000.0
              },
              {
                "code": "CW",
                "totalTxnCount": 68,
                "amountInvolved": 870000.0
              }
            ]
          },
          "narrative": "Financial transactions formed 75.72% of total activity, above the agent average."
        },
        "commissionSummary": {
          "overall": {
            "totalCommissionEarned": {
              "value": 12450.75,
              "currency": "INR"
            },
            "totalEligibleTxnCount": 171,
            "eligibleTxnAmount": {
              "value": 1619000.0,
              "currency": "INR"
            },
            "commissionRank": {
              "rank": 18,
              "peerGroup": "BR001",
              "movement": 3
            },
            "totalTxnCount": 243,
            "averageCommissionEarned": {
              "value": 72.81,
              "currency": "INR"
            }
          },
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "commissionEarned": {
                  "value": 1525.5,
                  "currency": "INR"
                },
                "eligibleTxnCount": 21,
                "commissionRank": 21
              }
            ]
          },
          "narrative": "Commission improved because eligible AEPS and cash deposit transactions increased."
        },
        "systemPerformance": {
          "overall": {
            "failureRatio": 2.88,
            "successRatio": 97.12,
            "businessDeclineCount": 5,
            "technicalDeclineCount": 2,
            "topBusinessDeclineResponseCode": {
              "code": "BD01",
              "label": "Insufficient balance",
              "count": 3
            },
            "topTechnicalDeclineResponseCode": {
              "code": "TD09",
              "label": "Issuer timeout",
              "count": 2
            },
            "band": "Low"
          },
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "successRatio": 96.77,
                "failureRatio": 3.23
              }
            ]
          },
          "narrative": "System health is stable; most declines are business declines, not technical errors."
        },
        "targetCampaignManagement": {
          "overall": {
            "totalTargetsAssigned": 3,
            "totalTargetAchievement": 68.4
          },
          "targets": [
            {
              "targetId": "TGT-AEPS-CW-MAY",
              "targetName": "AEPS Cash Withdrawal May Campaign",
              "achievementPercent": 68.4,
              "targetStatus": "Average",
              "txnPerDayRequired": 10,
              "pendingTxnCount": 126,
              "remainingDays": 12
            }
          ],
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "achievementPercent": 64.2,
                "targetStatus": "Average"
              }
            ]
          },
          "narrative": "Complete about 10 more AEPS cash withdrawal transactions per day to finish on time."
        },
        "anomalyRiskManagement": {
          "overall": {
            "suspiciousTxnRatio": 1.65,
            "totalSuspiciousTxnCount": 4,
            "totalAnomalyCaseCount": 2,
            "suspiciousCustomerCount": 3,
            "suspiciousAmount": {
              "value": 44000.0,
              "currency": "INR"
            },
            "topAnomalyCase": {
              "code": "ODD_TIME_TXN",
              "label": "Odd time transaction",
              "count": 2
            },
            "riskBand": "Very Low Risk"
          },
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "suspiciousTxnCount": 1,
                "riskBand": "Very Low Risk"
              }
            ]
          },
          "narrative": "Risk is low; the main anomaly pattern is odd-time transactions."
        },
        "agentAuditManagement": {
          "overall": {
            "totalAuditsInitiated": 2,
            "auditedMarksScored": 84.0,
            "ongoingAuditCount": 1,
            "lastAuditedOn": "2026-05-11"
          },
          "recentAudits": [
            {
              "auditId": "AUD-2026-0511-01",
              "auditName": "ARM-triggered risk audit",
              "auditSource": "ARM",
              "auditType": "Risk Based",
              "marksScored": 84.0,
              "status": "CLOSED"
            }
          ],
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "ongoingAuditCount": 1
              }
            ]
          },
          "narrative": "Latest audit result is High; one audit remains open."
        },
        "analyticsReportingManagement": {
          "overall": {
            "totalRecommendations": 2,
            "majorRecommendationCount": 1,
            "minorRecommendationCount": 1
          },
          "recommendations": [
            {
              "module": "TCM",
              "type": "PRESCRIPTIVE",
              "severity": "Major",
              "message": "Increase AEPS cash withdrawal by 10 transactions per day."
            },
            {
              "module": "ARM",
              "type": "DIAGNOSTIC",
              "severity": "Minor",
              "message": "Most suspicious activity is linked to odd-time transactions."
            }
          ],
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "totalRecommendations": 1,
                "topModule": "TCM"
              }
            ]
          }
        },
        "enterpriseAgentPerformanceRiskManagement": {
          "overall": {
            "iScore": 842,
            "band": "Stable",
            "operationalEfficiencyScore": 88.2,
            "behaviouralEfficiencyScore": 81.5,
            "complianceScore": 86.0,
            "financialHealthScore": 79.7,
            "totalCustomersEngaged": 96,
            "attendancePercentage": 94.0
          },
          "trendSummary": {
            "grain": "DAY",
            "periods": [
              {
                "periodCode": "2026-05-13",
                "iScore": 834,
                "band": "Stable"
              }
            ]
          },
          "narrative": "The agent is stable, with strong operations and good attendance."
        }
      }
    },
    {
      "code": "LAST_3_MONTHS",
      "label": "Last 3 Months",
      "fromDate": "2026-03-01",
      "toDate": "2026-05-31",
      "grain": "MONTH",
      "periodStatus": "COMPLETED",
      "sections": {
        "transactionSummary": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "periodLabel": "Mar 2026",
                "totalTxnCount": 920,
                "amountInvolved": {
                  "value": 6810000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "2026-04",
                "periodLabel": "Apr 2026",
                "totalTxnCount": 1015,
                "amountInvolved": {
                  "value": 7425000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "2026-05",
                "periodLabel": "May 2026",
                "totalTxnCount": 832,
                "amountInvolved": {
                  "value": 5980000.0,
                  "currency": "INR"
                }
              }
            ]
          },
          "narrative": "Monthly transaction volume peaked in April and remains healthy in May month-to-date."
        },
        "commissionSummary": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "commissionEarned": {
                  "value": 43800.0,
                  "currency": "INR"
                },
                "commissionRank": 24
              },
              {
                "periodCode": "2026-04",
                "commissionEarned": {
                  "value": 51250.0,
                  "currency": "INR"
                },
                "commissionRank": 19
              },
              {
                "periodCode": "2026-05",
                "commissionEarned": {
                  "value": 39400.0,
                  "currency": "INR"
                },
                "commissionRank": 18
              }
            ]
          },
          "narrative": "Commission rank improved from 24 to 18 over the last three months."
        },
        "systemPerformance": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "successRatio": 96.4,
                "failureRatio": 3.6,
                "band": "Moderate"
              },
              {
                "periodCode": "2026-04",
                "successRatio": 97.0,
                "failureRatio": 3.0,
                "band": "Low"
              },
              {
                "periodCode": "2026-05",
                "successRatio": 97.12,
                "failureRatio": 2.88,
                "band": "Low"
              }
            ]
          },
          "narrative": "System performance is improving month over month."
        },
        "targetCampaignManagement": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "totalTargetAchievement": 61.2,
                "targetStatus": "Average"
              },
              {
                "periodCode": "2026-04",
                "totalTargetAchievement": 66.7,
                "targetStatus": "Average"
              },
              {
                "periodCode": "2026-05",
                "totalTargetAchievement": 68.4,
                "targetStatus": "Average"
              }
            ]
          },
          "narrative": "Target achievement is improving, but daily pace still needs attention."
        },
        "anomalyRiskManagement": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "suspiciousTxnRatio": 2.4,
                "riskBand": "Very Low Risk"
              },
              {
                "periodCode": "2026-04",
                "suspiciousTxnRatio": 1.9,
                "riskBand": "Very Low Risk"
              },
              {
                "periodCode": "2026-05",
                "suspiciousTxnRatio": 1.65,
                "riskBand": "Very Low Risk"
              }
            ]
          },
          "narrative": "Risk ratio reduced across the last three months."
        },
        "agentAuditManagement": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "auditsInitiated": 1,
                "averageMarksScored": 78.0
              },
              {
                "periodCode": "2026-04",
                "auditsInitiated": 1,
                "averageMarksScored": 82.0
              },
              {
                "periodCode": "2026-05",
                "auditsInitiated": 2,
                "averageMarksScored": 84.0
              }
            ]
          },
          "narrative": "Audit score improved to 84 in May."
        },
        "analyticsReportingManagement": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "totalRecommendations": 5,
                "majorCount": 2
              },
              {
                "periodCode": "2026-04",
                "totalRecommendations": 4,
                "majorCount": 1
              },
              {
                "periodCode": "2026-05",
                "totalRecommendations": 2,
                "majorCount": 1
              }
            ]
          },
          "narrative": "Recommendation volume is reducing, showing better portfolio stability."
        },
        "enterpriseAgentPerformanceRiskManagement": {
          "trendSummary": {
            "grain": "MONTH",
            "periods": [
              {
                "periodCode": "2026-03",
                "iScore": 812,
                "band": "Stable"
              },
              {
                "periodCode": "2026-04",
                "iScore": 831,
                "band": "Stable"
              },
              {
                "periodCode": "2026-05",
                "iScore": 842,
                "band": "Stable"
              }
            ]
          },
          "narrative": "iScore improved steadily across the last three months."
        }
      }
    },
    {
      "code": "CURRENT_FY_QUARTERS",
      "label": "Current Financial Year Quarters",
      "fromDate": "2026-04-01",
      "toDate": "2027-03-31",
      "grain": "QUARTER",
      "periodStatus": "ACTIVE",
      "sections": {
        "transactionSummary": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "totalTxnCount": 2680,
                "amountInvolved": {
                  "value": 19930000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "totalTxnCount": 2920,
                "amountInvolved": {
                  "value": 21850000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "totalTxnCount": 3150,
                "amountInvolved": {
                  "value": 23600000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "totalTxnCount": 3360,
                "amountInvolved": {
                  "value": 25100000.0,
                  "currency": "INR"
                }
              }
            ]
          }
        },
        "commissionSummary": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "commissionEarned": {
                  "value": 134450.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "commissionEarned": {
                  "value": 148000.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "commissionEarned": {
                  "value": 159500.0,
                  "currency": "INR"
                }
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "commissionEarned": {
                  "value": 171200.0,
                  "currency": "INR"
                }
              }
            ]
          }
        },
        "systemPerformance": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "successRatio": 97.1,
                "band": "Low"
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "successRatio": 97.4,
                "band": "Low"
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "successRatio": 97.8,
                "band": "Very Low"
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "successRatio": 98.0,
                "band": "Very Low"
              }
            ]
          }
        },
        "targetCampaignManagement": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "totalTargetAchievement": 68.4,
                "targetStatus": "Average"
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "totalTargetAchievement": 78.0,
                "targetStatus": "High"
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "totalTargetAchievement": 86.0,
                "targetStatus": "High"
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "totalTargetAchievement": 94.0,
                "targetStatus": "High"
              }
            ]
          }
        },
        "anomalyRiskManagement": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "suspiciousTxnRatio": 1.65,
                "riskBand": "Very Low Risk"
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "suspiciousTxnRatio": 1.4,
                "riskBand": "Very Low Risk"
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "suspiciousTxnRatio": 1.2,
                "riskBand": "Very Low Risk"
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "suspiciousTxnRatio": 1.0,
                "riskBand": "Very Low Risk"
              }
            ]
          }
        },
        "agentAuditManagement": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "auditsInitiated": 4,
                "averageMarksScored": 84.0
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "auditsInitiated": 3,
                "averageMarksScored": 86.0
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "auditsInitiated": 3,
                "averageMarksScored": 88.0
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "auditsInitiated": 2,
                "averageMarksScored": 90.0
              }
            ]
          }
        },
        "analyticsReportingManagement": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodStatus": "ACTIVE",
                "totalRecommendations": 11,
                "majorCount": 4
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodStatus": "FORECAST",
                "totalRecommendations": 9,
                "majorCount": 3
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodStatus": "FORECAST",
                "totalRecommendations": 7,
                "majorCount": 2
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodStatus": "FORECAST",
                "totalRecommendations": 5,
                "majorCount": 1
              }
            ]
          }
        },
        "enterpriseAgentPerformanceRiskManagement": {
          "trendSummary": {
            "grain": "QUARTER",
            "periods": [
              {
                "periodCode": "FY2026-27-Q1",
                "periodLabel": "Q1 FY 2026-27",
                "fromDate": "2026-04-01",
                "toDate": "2026-06-30",
                "periodStatus": "ACTIVE",
                "iScore": 842,
                "band": "Stable"
              },
              {
                "periodCode": "FY2026-27-Q2",
                "periodLabel": "Q2 FY 2026-27",
                "fromDate": "2026-07-01",
                "toDate": "2026-09-30",
                "periodStatus": "FORECAST",
                "iScore": 855,
                "band": "Stable"
              },
              {
                "periodCode": "FY2026-27-Q3",
                "periodLabel": "Q3 FY 2026-27",
                "fromDate": "2026-10-01",
                "toDate": "2026-12-31",
                "periodStatus": "FORECAST",
                "iScore": 872,
                "band": "Stable"
              },
              {
                "periodCode": "FY2026-27-Q4",
                "periodLabel": "Q4 FY 2026-27",
                "fromDate": "2027-01-01",
                "toDate": "2027-03-31",
                "periodStatus": "FORECAST",
                "iScore": 890,
                "band": "Stable"
              }
            ]
          }
        }
      }
    }
  ],
  "recommendations": [
    {
      "module": "TCM",
      "type": "PRESCRIPTIVE",
      "priority": "Major",
      "message": "Prioritize AEPS cash withdrawal activity for the next 12 days."
    }
  ]
}
```

## GraphQL Schema
```graphql
type Query {
  agentPortfolioSummary(input: AgentPortfolioSummaryInput!): AgentPortfolioSummary!
  agentPortfolioSection(input: AgentPortfolioSectionInput!): PortfolioSectionResponse!
  agentPortfolioTrends(input: AgentPortfolioTrendInput!): AgentPortfolioTrendResponse!
}

input AgentPortfolioSummaryInput {
  agentCode: ID!
  windowCodes: [WindowCode!] = [LAST_7_DAYS, LAST_3_MONTHS, CURRENT_FY_QUARTERS]
  sectionCodes: [SectionCode!]
  includeTrends: Boolean = true
  includeBreakdowns: Boolean = true
  includeNarratives: Boolean = true
  filters: PortfolioFiltersInput
}

enum WindowCode { LAST_7_DAYS LAST_3_MONTHS CURRENT_FY_QUARTERS CUSTOM }
enum TrendGrain { DAY MONTH QUARTER }
enum SectionCode {
  TRANSACTION_SUMMARY
  COMMISSION_SUMMARY
  SYSTEM_PERFORMANCE
  TARGET_CAMPAIGN_MANAGEMENT
  ANOMALY_RISK_MANAGEMENT
  AGENT_AUDIT_MANAGEMENT
  ANALYTICS_REPORTING_MANAGEMENT
  ENTERPRISE_AGENT_PERFORMANCE_RISK_MANAGEMENT
}

type AgentPortfolioSummary {
  requestId: ID!
  schemaVersion: String!
  generatedAt: DateTime!
  agent: Agent!
  portfolioStatus: PortfolioStatus!
  timeWindows: [TimeWindowSummary!]!
  recommendations: [Recommendation!]!
}

type TimeWindowSummary {
  code: WindowCode!
  label: String!
  fromDate: Date!
  toDate: Date!
  grain: TrendGrain!
  sections: PortfolioSections!
}

type PortfolioSections {
  transactionSummary: TransactionSummarySection
  commissionSummary: CommissionSummarySection
  systemPerformance: SystemPerformanceSection
  targetCampaignManagement: TargetCampaignManagementSection
  anomalyRiskManagement: AnomalyRiskManagementSection
  agentAuditManagement: AgentAuditManagementSection
  analyticsReportingManagement: AnalyticsReportingManagementSection
  enterpriseAgentPerformanceRiskManagement: EAPRMSection
}
```

## GraphQL Query
```graphql
query AgentPortfolioSummary($input: AgentPortfolioSummaryInput!) {
  agentPortfolioSummary(input: $input) {
    requestId
    generatedAt
    agent { agentCode displayName status branchCode regionCode }
    portfolioStatus { overallBand dailyRefreshStatus }
    timeWindows {
      code
      label
      grain
      sections {
        transactionSummary {
          overall {
            totalTxnCount
            financialTxnRatio
            amountInvolved { value currency }
          }
          trendSummary {
            periods { periodCode periodLabel totalTxnCount }
          }
          narrative
        }
        commissionSummary {
          overall {
            totalCommissionEarned { value currency }
            commissionRank { rank peerGroup movement }
          }
        }
        enterpriseAgentPerformanceRiskManagement {
          overall { iScore band attendancePercentage }
          narrative
        }
      }
    }
    recommendations { module type priority message }
  }
}
```

## GraphQL Variables
```json
{
  "input": {
    "agentCode": "BC00012345",
    "windowCodes": [
      "LAST_7_DAYS",
      "LAST_3_MONTHS",
      "CURRENT_FY_QUARTERS"
    ],
    "sectionCodes": [
      "TRANSACTION_SUMMARY",
      "COMMISSION_SUMMARY",
      "ENTERPRISE_AGENT_PERFORMANCE_RISK_MANAGEMENT"
    ],
    "includeTrends": true,
    "includeBreakdowns": true,
    "includeNarratives": true,
    "filters": {
      "channelCodes": [
        "ONUS",
        "OFFUS"
      ],
      "transactionTypeCodes": [
        "AEPS",
        "RUPAY"
      ],
      "serviceTypeCodes": [
        "CD",
        "CW"
      ]
    }
  }
}
```

## GraphQL Filled Response
```json
{
  "data": {
    "agentPortfolioSummary": {
      "requestId": "req_01HY9Z6N0W5D4Z7D6T7E8K9P2A",
      "generatedAt": "2026-05-20T09:30:00+05:30",
      "agent": {
        "agentCode": "BC00012345",
        "displayName": "Sample BC Agent",
        "status": "ACTIVE",
        "branchCode": "BR001",
        "regionCode": "RG-SOUTH"
      },
      "portfolioStatus": {
        "overallBand": "Stable",
        "dailyRefreshStatus": "COMPLETED"
      },
      "timeWindows": [
        {
          "code": "LAST_7_DAYS",
          "label": "Last 7 Days",
          "grain": "DAY",
          "sections": {
            "transactionSummary": {
              "overall": {
                "totalTxnCount": 243,
                "financialTxnRatio": 75.72,
                "amountInvolved": {
                  "value": 1854000.0,
                  "currency": "INR"
                }
              },
              "trendSummary": {
                "periods": [
                  {
                    "periodCode": "2026-05-13",
                    "periodLabel": "13 May 2026",
                    "totalTxnCount": 31
                  }
                ]
              },
              "narrative": "Financial transactions formed 75.72% of total activity."
            },
            "commissionSummary": {
              "overall": {
                "totalCommissionEarned": {
                  "value": 12450.75,
                  "currency": "INR"
                },
                "commissionRank": {
                  "rank": 18,
                  "peerGroup": "BR001",
                  "movement": 3
                }
              }
            },
            "enterpriseAgentPerformanceRiskManagement": {
              "overall": {
                "iScore": 842,
                "band": "Stable",
                "attendancePercentage": 94.0
              },
              "narrative": "The agent is stable, with strong operations and good attendance."
            }
          }
        }
      ],
      "recommendations": [
        {
          "module": "TCM",
          "type": "PRESCRIPTIVE",
          "priority": "Major",
          "message": "Prioritize AEPS cash withdrawal activity."
        }
      ]
    }
  }
}
```