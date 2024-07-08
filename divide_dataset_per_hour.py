import pandas as pd
from tqdm import tqdm


n9_path = 'D:/FV/VIU/clean_data/9n_9ngranmarchaporlajusticia.csv'
noAlTar_path = 'D:/FV/VIU/clean_data/noaltarifazo_ruidazonacional.csv'

def save_df_perHour(df, path):
    df['hashtag'] = df.hashtag.str.lower(
                        ).str.normalize('NFKD'
                        ).str.encode('ascii', errors='ignore'
                        ).str.decode('utf-8')

    for g in tqdm(df.groupby('hour')):
        g[1].loc[:,['user','hashtag']].to_csv(path.format(g[0]))

n9 = pd.read_csv(n9_path)
save_df_perHour(n9, 'D:/FV/VIU/clean_data/clean_datasets_perHour/9n_{}.csv')

noAlTar_df = pd.read_csv(noAlTar_path)
save_df_perHour(noAlTar_df, 'D:/FV/VIU/clean_data/clean_datasets_perHour/no_AlTar_{}.csv')
