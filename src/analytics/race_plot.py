import pandas as pd
import bar_chart_race as bcr

df = pd.read_csv("../../data/posts_race_df.csv")
df = df.set_index("dtReference")

bcr.bar_chart_race(df=df,
                   filename="../../data/posts_race_linkedin.mp4",
                   n_bars=10,
                   dpi=180,
                   cmap='dark12',
                   title='Quantidade de posts totais no TabNews',
                   steps_per_period=30,
                   period_length=100,
                   filter_column_colors=True,
                   figsize=(4, 4),
)
