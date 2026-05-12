def insert_hr_employee(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.employees (
            subsidiary_id, employee_id, first_name, last_name, tin, sss_number,
            philhealth_number, pagibig_number, department, position,
            employment_type, date_hired, basic_salary, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("employee_id"),
            record.get("first_name"),
            record.get("last_name"),
            record.get("tin"),
            record.get("sss_number"),
            record.get("philhealth_number"),
            record.get("pagibig_number"),
            record.get("department"),
            record.get("position"),
            record.get("employment_type"),
            record.get("date_hired"),
            record.get("basic_salary"),
        ),
    )


def insert_payroll_run(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.payroll_runs (
            subsidiary_id, payroll_period, employee_id, gross_pay,
            total_deductions, net_pay, payroll_date, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("payroll_period"),
            record.get("employee_id"),
            record.get("gross_pay"),
            record.get("total_deductions"),
            record.get("net_pay"),
            record.get("payroll_date"),
        ),
    )


def insert_statutory_contribution(cur, record):
    cur.execute(
        """
        INSERT INTO bronze.statutory_contributions (
            subsidiary_id, period_month, period_year,
            sss_employer, sss_employee,
            philhealth_employer, philhealth_employee,
            pagibig_employer, pagibig_employee,
            total_amount, ingested_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT DO NOTHING
        """,
        (
            record.get("subsidiary_id"),
            record.get("period_month"),
            record.get("period_year"),
            record.get("sss_employer"),
            record.get("sss_employee"),
            record.get("philhealth_employer"),
            record.get("philhealth_employee"),
            record.get("pagibig_employer"),
            record.get("pagibig_employee"),
            record.get("total_amount"),
        ),
    )
