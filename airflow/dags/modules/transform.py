import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import numpy as np

class transform():
    def __init__(self, engine_sql, engine_postgres):
        self.engine_sql = engine_sql
        self.engine_postgres = engine_postgres

    def get_mysql_data(self):
        sql = "SELECT * FROM covid_data"
        df = pd.read_sql(sql, con=self.engine_sql)
        print("Get MySQL Data Success")
        return df

    def create_dim_province(self):
        df = self.get_mysql_data()
        df_province = df[['kode_prov', 'nama_prov']]
        df_province = df_province.rename(columns={'kode_prov':'province_id', 'nama_prov':'province_name'})
        df_province = df_province.drop_duplicates()

        try:
            p = "DROP TABLE IF EXISTS dim_province"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

        df_province.to_sql(con=self.engine_postgres, name='dim_province', index=False)
        print('INSERT PROVINCE TO POSTGRES SUCCESSFULLY') 
    
    def create_dim_district(self):
        df = self.get_mysql_data()
        df_district = df[['kode_kab', 'kode_prov', 'nama_kab']]
        df_district = df_district.rename(columns={'kode_kab':'district_id', 'kode_prov':'province_id', 'nama_kab':'district_name'})
        df_district = df_district.drop_duplicates()     
        
        try:
            p = "DROP TABLE IF EXISTS dim_district"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

        df_district.to_sql(con=self.engine_postgres, name='dim_district', index=False)
        print('INSERT DISTRICT TO POSTGRES SUCCESSFULLY')

    def create_dim_case(self):
        df = self.get_mysql_data()
        column_start = ['closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 'confirmation_meninggal', 'closecontact_discarded', 'confirmation_meninggal', 'confirmation_sembuh', 'probable_diisolasi', 'probable_discarded', 'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal']
        column_end = ['id', 'status_name', 'status_detail', 'status']

        df = df[column_start]
        df = df[:1]
        df = df.melt(var_name='status', value_name='total')
        df = df.drop_duplicates('status').sort_values('status')

        df['id'] = np.arange(1, df.shape[0]+1)
        df[['status_name', 'status_detail']] = df['status'].str.split('_', n=1, expand=True)

        df = df[column_end]

        try:
            p = "DROP TABLE IF EXISTS dim_case"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

        df.to_sql(con=self.engine_postgres, name='dim_case', index=False)
        
        print('INSERT CASE TO POSTGRES SUCCESSFULLY')

        return df
    
    def create_province_daily(self):
        df = self.get_mysql_data()
        df_dim_case = self.create_dim_case()

        column_start = ['tanggal', 'kode_prov', 'closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 'confirmation_meninggal', 'closecontact_discarded', 'confirmation_meninggal', 'confirmation_sembuh', 'probable_diisolasi', 'probable_discarded', 'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal']
        column_end = ['date', 'province_id', 'status', 'total']

        data = df[column_start]
        data = data.melt(id_vars = ['tanggal', 'kode_prov'], var_name = 'status', value_name = 'total').sort_values(['tanggal', 'kode_prov'])
        data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_dim_case = df_dim_case.rename({'id':'case_id'}, axis = 1)

        data = pd.merge(data, df_dim_case, how='inner', on = 'status')
        data = data[['id', 'province_id', 'case_id', 'date', 'total']]

        try:
            p = "DROP TABLE IF EXISTS province_daily"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

        data.to_sql(con=self.engine_postgres, name='province_daily', index=False)
        
        print('INSERT PROVINCE_DAILY TO POSTGRES SUCCESSFULLY')
       
    def create_district_daily(self):
        df = self.get_mysql_data()
        df_dim_case = self.create_dim_case()

        column_start = ['tanggal', 'kode_kab', 'closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 'confirmation_meninggal', 'closecontact_discarded', 'confirmation_meninggal', 'confirmation_sembuh', 'probable_diisolasi', 'probable_discarded', 'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal']
        column_end = ['date', 'district_id', 'status', 'total']

        data = df[column_start]
        data = data.melt(id_vars = ['tanggal', 'kode_kab'], var_name = 'status', value_name = 'total').sort_values(['tanggal', 'kode_kab'])
        data = data.groupby(by=['tanggal', 'kode_kab', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_dim_case = df_dim_case.rename({'id':'case_id'}, axis = 1)

        data = pd.merge(data, df_dim_case, how='inner', on = 'status')
        data = data[['id', 'district_id', 'case_id', 'date', 'total']]

        try:
            p = "DROP TABLE IF EXISTS district_daily"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

        data.to_sql(con=self.engine_postgres, name='district_daily', index=False)
        
        print('INSERT DISTRICT_DAILY TO POSTGRES SUCCESSFULLY')
