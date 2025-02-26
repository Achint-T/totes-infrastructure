from src.transform_utils.dim_staff import util_dim_staff
import pytest 
import pandas as pd

class TestDimStaff:
    def test_returns_dataframe(self):
        df_fact_sales_order = pd.DataFrame({"staff_id": [1, 2, 3, 4]})

        df_staff = pd.DataFrame([
            [ "Jeremie", "Franey", "jeremie.franey@terrifictotes.com", 101],
            ["Deron", "Beier", "deron.beier@terrifictotes.com", 102],
            [ "Jeanette", "Erdman", "jeanette.erdman@terrifictotes.com", 102],
            [ "Ana", "Glover", "ana.glover@terrifictotes.com", 103]
        ], columns=[ "first_name", "last_name", "email_address", "department_id"])

        df_department = pd.DataFrame([
            [101, "Purchasing", "Manchester"],
            [102, "Facilities", "Manchester"],
            [103, "Production", "Leeds"]
        ], columns=["department_id", "department_name", "location"])

        output = util_dim_staff(df_fact_sales_order, df_staff,df_department )

        assert isinstance(output, pd.DataFrame)

    def test_returns_correct_columns(self):
        df_fact_sales_order = pd.DataFrame({"staff_id": [1, 2, 3, 4]})

        df_staff = pd.DataFrame([
            [ "Jeremie", "Franey", "jeremie.franey@terrifictotes.com", 101],
            ["Deron", "Beier", "deron.beier@terrifictotes.com", 102],
            [ "Jeanette", "Erdman", "jeanette.erdman@terrifictotes.com", 102],
            [ "Ana", "Glover", "ana.glover@terrifictotes.com", 103]
        ], columns=[ "first_name", "last_name", "email_address", "department_id"])

        df_department = pd.DataFrame([
            [101, "Purchasing", "Manchester"],
            [102, "Facilities", "Manchester"],
            [103, "Production", "Leeds"]
        ], columns=["department_id", "department_name", "location"])


        output = util_dim_staff(df_fact_sales_order, df_staff,df_department )
            
        assert list(output.columns) == ["staff_id", "first_name", "last_name", "email_address",
                            "department_name", "location"]

class TestDimStaffErrorHandling:
    def test_empty_dataframe_fact_sales(self):
        df_fact_sales_order = pd.DataFrame() 
        df_staff = pd.DataFrame([
            ["Jeremie", "Franey", "jeremie.franey@terrifictotes.com", 101],
            ["Deron", "Beier", "deron.beier@terrifictotes.com", 102]
        ], columns=["first_name", "last_name", "email_address", "department_id"])

        df_department = pd.DataFrame([
            [101, "Purchasing", "Manchester"]
        ], columns=["department_id", "department_name", "location"])

        output = util_dim_staff(df_fact_sales_order, df_staff, df_department)
        assert output == "The source dataframe for df_fact_sales_order is empty"

    def test_empty_dataframe_staff(self):
        df_fact_sales_order = pd.DataFrame({"staff_id": [1, 2]})
        df_staff = pd.DataFrame() 
        df_department = pd.DataFrame([
            [101, "Purchasing", "Manchester"]
        ], columns=["department_id", "department_name", "location"])

        with pytest.raises(ValueError, match="The source dataframe for df_staff is empty"):
            util_dim_staff(df_fact_sales_order, df_staff, df_department)

    def test_empty_dataframe_department(self):
        df_fact_sales_order = pd.DataFrame({"staff_id": [1, 2]})
        df_staff = pd.DataFrame([
            ["Jeremie", "Franey", "jeremie.franey@terrifictotes.com", 101]
        ], columns=["first_name", "last_name", "email_address", "department_id"])
        df_department = pd.DataFrame() 

        with pytest.raises(ValueError, match="The source dataframe for df_department is empty"):
            util_dim_staff(df_fact_sales_order, df_staff, df_department)

    def test_missing_columns_staff(self):
        df_fact_sales_order = pd.DataFrame({"staff_id": [1, 2]})
        df_staff = pd.DataFrame([  # missing email_address
            ["Jeremie", "Franey", 101]
        ], columns=["first_name", "last_name", "department_id"])
        df_department = pd.DataFrame([
            [101, "Purchasing", "Manchester"]
        ], columns=["department_id", "department_name", "location"])

        with pytest.raises(KeyError, match="Missing columns in df_staff: email_address"):
            util_dim_staff(df_fact_sales_order, df_staff, df_department)

    def test_missing_columns_department(self):
        df_fact_sales_order = pd.DataFrame({"staff_id": [1, 2]})
        df_staff = pd.DataFrame([
            ["Jeremie", "Franey", "jeremie.franey@terrifictotes.com", 101]
        ], columns=["first_name", "last_name", "email_address", "department_id"])
        df_department = pd.DataFrame([  # missing location
            [101, "Purchasing"]
        ], columns=["department_id", "department_name"])
