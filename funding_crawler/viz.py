import polars as pl
from matplotlib import pyplot as plt
import seaborn as sns


def count_plot(df, col):
    lst = [x for xs in df[col] if xs is not None for x in xs]

    series = pl.Series(col, lst)
    counts_df = (
        pl.DataFrame([series])
        .unpivot()
        .group_by("value")
        .len()
        .sort("len", descending=True)
        .rename({"value": col, "len": "count"})
    )

    sns.barplot(data=counts_df, y=col, x="count", order=counts_df[col])
    plt.show()


def perc_comp(df_a, df_b, col, title):
    """Display bar chart with percentages of col categories for df a and compare with df b by displaying dots"""

    def get_percs(df, col):
        total = len(df)
        perc_dct = {}
        for cat in df.select(pl.col(col).explode().unique()).to_series().to_list():
            # Count rows where the category is in the list
            n = len(df.filter(pl.col(col).list.contains(cat)))
            perc = (n / total) * 100
            perc_dct[cat] = perc
        return perc_dct

    total_percs = get_percs(df_b, col)
    recent_percs = get_percs(df_a, col)

    sorted_recent = sorted(recent_percs.items(), key=lambda x: x[1], reverse=False)
    categories = [item[0] for item in sorted_recent]
    recent_values = [item[1] for item in sorted_recent]

    total_values = [total_percs.get(cat, 0) for cat in categories]

    plot_data = pl.DataFrame(
        {
            "category": categories,
            "recently_deleted": recent_values,
            "before": total_values,
        }
    )

    _, ax = plt.subplots(figsize=(12, 8))

    y_pos = range(len(categories))
    bars = ax.barh(y_pos, plot_data["recently_deleted"], color="steelblue")  # noqa

    for i, (cat, recent, total) in enumerate(
        zip(categories, recent_values, total_values)
    ):
        ax.plot(total, i, "ro", markersize=8, label="All Data" if i == 0 else "")
        ax.text(recent + 0.2, i, f"{recent:.1f}%", va="center")

    ax.grid(axis="x", linestyle="--", alpha=0.7)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(categories)

    ax.set_xlabel("Percentage")
    ax.set_ylabel(col)
    ax.set_title(title)

    ax.legend(["Percentage of previous data"], loc="lower right")

    plt.tight_layout()
    plt.show()
