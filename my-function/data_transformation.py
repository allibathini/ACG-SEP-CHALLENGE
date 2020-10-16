import pandas as pd
    #df_data_transform(NYTData,JOhnHopkinsData,'date','inner')
def df_data_transform(df1, df2,join_on,join_type,initial_load_flag,max_date):
    try:
        df1['date'] = pd.to_datetime(df1['date'], format='%Y-%m-%d', errors='raise')
        df2['Date'] = pd.to_datetime(df2['Date'], format='%Y-%m-%d', errors='raise')
        df_johnhopkins_filtered =df2[df2['Country/Region']=='US']
        df_johnhopkins_renamed = df_johnhopkins_filtered.rename(columns={"Date": "date"})
        if initial_load_flag == 'False':
            print("########## Performing Incremental Load ###########")
            df1 = df1[df1['date']> max_date]
            df_johnhopkins_renamed = df_johnhopkins_renamed[df_johnhopkins_renamed['date']>max_date]

        output_merged = pd.merge(df1, df_johnhopkins_renamed[['date', 'Recovered']], on=join_on, how=join_type)
        return output_merged

    except:
        print ("Failed while performing transformations")

