# -*- coding: utf-8 -*-

import pandas as pd

"""File awal"""

df = pd.read_csv('/content/marketing_data.csv')
df

"""## BASIC OOP"""

class MarketingDataETL:
    def __init__(self, df):
        self.df = df
        self.df = pd.read_csv(df, sep=';')

    def transform(self):
                # Mengubah format tanggal
                self.df['purchase_date'] = pd.to_datetime(self.df['purchase_date']).dt.strftime('%Y-%m-%d')
                # Membersihkan nilai yang kosong (NaN)
                self.df = self.df.dropna()

    def store(self, output_file):
                self.df.to_csv(output_file, index=False)

# Membuat objek MarketingDataETL
etl_processor = MarketingDataETL('/content/marketing_data.csv')

# Melakukan transformasi
etl_processor.transform()

# Menyimpan data yang telah ditransformasi
etl_processor.store("transformed_marketing_data.csv")

#Membaca data baru yang telah ditransformasikan
data_transformed = pd.read_csv("transformed_marketing_data.csv")
print(data_transformed)

"""## INHIRITANCE AND POLYMORPHISM"""

class TargetedMarketingETL(MarketingDataETL):
    def __init__(self, df):
        MarketingDataETL.__init__(self, df)
        pass

    def segment_customers(self):
        self.df = self.segments = self.df.groupby('customer_id', as_index=False)['amount_spent'].mean()

    def transform(self):
        if self.df is not None:
            if 'purchase_date' in self.df.columns:
                self.df['purchase_date'] = pd.to_datetime(self.df['purchase_date']).dt.strftime('%Y-%m-%d')
            self.df.fillna('NaN', inplace=True)

    def stored(self, output_file):
                self.df.to_csv(output_file, index=False)

etl = TargetedMarketingETL('marketing_data.csv')
etl.transform()
etl.segment_customers()
etl.stored("new_file_category.csv")
data_transformed = pd.read_csv("new_file_category.csv", sep = ',')
print(data_transformed)