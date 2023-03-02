# recognized-revenue-monthly-calculator

Given two tables, [payments] and [school_plans], this SQL script will calculate monthly recognized revenue (tax liability) based on a monthly and annual subscription based business model.

Recognized revenue is defined as the pro-rated amount of a monthly or annual subscription payment that applies to a given month. For example, if a monthly subscription payment of $39 is made on October 15, the recognized revenue for October would be 17/31 * 39 (seventeen days in October from the 15th to the 31st, divided by 31 days in October, times the payment amount). The same payment would account for 14/31 * 39 in November’s recognized revenue.
