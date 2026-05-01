class reuse:
    def drop(self,df,columns):
        df=df.drop(*columns)
        return df
