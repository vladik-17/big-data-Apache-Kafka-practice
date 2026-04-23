"""
visualize.py

Скрипт визуализации результатов аналитики погоды в Торжке.
Читает выходные файлы Apache Hive, сохранённые в HDFS и скачанные на локальную машину.

Полный пайплайн: dataset.csv -> Kafka (batch producer) -> HDFS -> Hive (SQL-запросы)
-> hive_output/ -> этот скрипт -> графики PNG + текстовый отчёт

Структура входных данных (папка hive_output/):
  yearly_stats/000000_0       — год, avg_temp, max_temp, min_temp
  monthly_stats/000000_0      — месяц, avg_temp, max_temp, min_temp
  precipitation_stats/000000_0 — месяц, avg_precip, max_precip, total_precip
  season_stats/000000_0       — сезон, avg_temp, max_temp, min_temp, avg_precip
"""

import pandas as pd
import matplotlib.pyplot as plt
import os
from datetime import datetime

# Пути к папкам — всё относительно расположения скрипта
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
HIVE_DIR    = os.path.join(BASE_DIR, "hive_output")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# Глобальные настройки стиля графиков
plt.rcParams.update({
    "figure.dpi": 120,
    "axes.titlesize": 14,
    "axes.labelsize": 11,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "axes.grid": True,
    "grid.alpha": 0.4,
})

# Русские названия месяцев для подписей осей
month_names = ["Янв","Фев","Мар","Апр","Май","Июн",
               "Июл","Авг","Сен","Окт","Ноя","Дек"]

# Порядок сезонов для корректной сортировки на графике
season_order = ["Зима", "Весна", "Лето", "Осень"]


# Загружаем выходные файлы Hive — CSV без заголовков (формат Hive по умолчанию)

yearly = pd.read_csv(
    os.path.join(HIVE_DIR, "yearly_stats", "000000_0"),
    header=None, names=["year", "avg_temp", "max_temp", "min_temp"]
)

monthly = pd.read_csv(
    os.path.join(HIVE_DIR, "monthly_stats", "000000_0"),
    header=None, names=["month", "avg_temp", "max_temp", "min_temp"]
)

precip = pd.read_csv(
    os.path.join(HIVE_DIR, "precipitation_stats", "000000_0"),
    header=None, names=["month", "avg_precip", "max_precip", "total_precip"]
)

seasons = pd.read_csv(
    os.path.join(HIVE_DIR, "season_stats", "000000_0"),
    header=None, names=["season", "avg_temp", "max_temp", "min_temp", "avg_precip"]
)
# Сортируем сезоны в правильном порядке
seasons["order"] = seasons["season"].map({s: i for i, s in enumerate(season_order)})
seasons = seasons.sort_values("order").reset_index(drop=True)

print("Данные загружены из hive_output/")


# График 1: Временной ряд температуры по годам
# Источник: Hive-запрос yearly_stats — агрегация AVG/MAX/MIN по году.
# Закрашенная область показывает годовой диапазон температур.

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(yearly["year"], yearly["avg_temp"], color="#1565C0", linewidth=2,
        marker="o", markersize=3, label="Средняя")
ax.plot(yearly["year"], yearly["max_temp"], color="#C62828", linewidth=1.2,
        linestyle="--", label="Максимальная")
ax.plot(yearly["year"], yearly["min_temp"], color="#283593", linewidth=1.2,
        linestyle="--", label="Минимальная")
ax.axhline(0, color="gray", linewidth=0.8, linestyle=":")
ax.fill_between(yearly["year"], yearly["min_temp"], yearly["max_temp"],
                alpha=0.08, color="#1565C0")
ax.set_title("Температура в Торжке по годам (2000–2025)")
ax.set_xlabel("Год")
ax.set_ylabel("Температура (°C)")
ax.legend()
plt.tight_layout()
path1 = os.path.join(RESULTS_DIR, "yearly_temperature.png")
plt.savefig(path1)
plt.close()
print(f"Сохранён: {path1}")


# График 2: Сезонность — средняя температура по месяцам
# Источник: Hive-запрос monthly_stats.
# Столбцы: красный = выше нуля, синий = ниже нуля.
# Усы показывают средний диапазон max/min по всем годам.

colors = ["#C62828" if t > 0 else "#1565C0" for t in monthly["avg_temp"]]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(month_names, monthly["avg_temp"], color=colors,
              edgecolor="white", linewidth=0.5)
ax.errorbar(month_names, monthly["avg_temp"],
            yerr=[monthly["avg_temp"] - monthly["min_temp"],
                  monthly["max_temp"] - monthly["avg_temp"]],
            fmt="none", color="black", capsize=4, linewidth=1)
ax.axhline(0, color="black", linewidth=0.8)
for bar, val in zip(bars, monthly["avg_temp"]):
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + (0.5 if val >= 0 else -1.5),
            f"{val:.1f}°", ha="center", va="bottom", fontsize=8)
ax.set_title("Средняя температура по месяцам (сезонность, 2000–2025)")
ax.set_xlabel("Месяц")
ax.set_ylabel("Температура (°C)")
plt.tight_layout()
path2 = os.path.join(RESULTS_DIR, "monthly_seasonality.png")
plt.savefig(path2)
plt.close()
print(f"Сохранён: {path2}")


# График 3: Осадки по месяцам
# Источник: Hive-запрос precipitation_stats.
# Показывает средние суточные осадки — какой месяц самый дождливый.

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(month_names, precip["avg_precip"],
              color="#0277BD", edgecolor="white", linewidth=0.5)
for i, val in enumerate(precip["avg_precip"]):
    ax.text(i, val + 0.05, f"{val:.1f}", ha="center", va="bottom", fontsize=8)
ax.set_title("Среднесуточные осадки по месяцам (Торжок, 2000–2025)")
ax.set_xlabel("Месяц")
ax.set_ylabel("Осадки (мм/день)")
plt.tight_layout()
path3 = os.path.join(RESULTS_DIR, "monthly_precipitation.png")
plt.savefig(path3)
plt.close()
print(f"Сохранён: {path3}")


# График 4: Температура и осадки по сезонам
# Источник: Hive-запрос season_stats (CASE WHEN по номеру месяца).
# Два графика рядом: температура слева, осадки справа.

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

season_colors = ["#5C6BC0", "#66BB6A", "#EF5350", "#FFA726"]

bars1 = ax1.bar(seasons["season"], seasons["avg_temp"],
                color=season_colors, edgecolor="white", linewidth=0.5)
ax1.errorbar(seasons["season"], seasons["avg_temp"],
             yerr=[seasons["avg_temp"] - seasons["min_temp"],
                   seasons["max_temp"] - seasons["avg_temp"]],
             fmt="none", color="black", capsize=5, linewidth=1.2)
ax1.axhline(0, color="black", linewidth=0.8)
for bar, val in zip(bars1, seasons["avg_temp"]):
    ax1.text(bar.get_x() + bar.get_width() / 2,
             bar.get_height() + (0.3 if val >= 0 else -1.2),
             f"{val:.1f}°", ha="center", va="bottom", fontsize=9)
ax1.set_title("Средняя температура по сезонам")
ax1.set_xlabel("Сезон")
ax1.set_ylabel("Температура (°C)")

bars2 = ax2.bar(seasons["season"], seasons["avg_precip"],
                color=season_colors, edgecolor="white", linewidth=0.5)
for bar, val in zip(bars2, seasons["avg_precip"]):
    ax2.text(bar.get_x() + bar.get_width() / 2, val + 0.03,
             f"{val:.2f}", ha="center", va="bottom", fontsize=9)
ax2.set_title("Среднесуточные осадки по сезонам")
ax2.set_xlabel("Сезон")
ax2.set_ylabel("Осадки (мм/день)")

plt.suptitle("Климат Торжка по сезонам (2000–2025)", fontsize=13, y=1.02)
plt.tight_layout()
path4 = os.path.join(RESULTS_DIR, "season_stats.png")
plt.savefig(path4, bbox_inches="tight")
plt.close()
print(f"Сохранён: {path4}")


# Текстовый отчёт с ключевыми метриками из результатов Hive.
# Имя файла содержит временную метку согласно требованиям задания.

now = datetime.now()
report_path = os.path.join(RESULTS_DIR, f"report_{now.strftime('%Y%m%d_%H%M%S')}.txt")

warmest_m  = monthly.loc[monthly["avg_temp"].idxmax()]
coldest_m  = monthly.loc[monthly["avg_temp"].idxmin()]
wettest_m  = precip.loc[precip["avg_precip"].idxmax()]
trend = yearly["avg_temp"].iloc[-5:].mean() > yearly["avg_temp"].iloc[:5].mean()

with open(report_path, "w", encoding="utf-8") as f:
    f.write(f"ОТЧЁТ: Погода в Торжке (2000–2025)\n")
    f.write(f"Сформирован: {now.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

    f.write("ИСТОЧНИК ДАННЫХ\n")
    f.write("  Пайплайн: dataset.csv -> Kafka (batch) -> HDFS -> Apache Hive\n")
    f.write("  Таблица Hive: weather_data (EXTERNAL TABLE, CSV)\n")
    f.write("  HDFS: /user/cloudera/weather_csv/dataset.csv\n")
    f.write("  Результаты Hive: /user/cloudera/hive_output/\n\n")

    f.write("ОБЩАЯ СТАТИСТИКА\n")
    f.write(f"  Период наблюдений:    {int(yearly['year'].min())} — {int(yearly['year'].max())}\n")
    f.write(f"  Количество лет:       {len(yearly)}\n\n")

    f.write("ТЕМПЕРАТУРА (из yearly_stats)\n")
    f.write(f"  Средняя за период:    {yearly['avg_temp'].mean():.2f} °C\n")
    f.write(f"  Абсолютный максимум:  {yearly['max_temp'].max():.1f} °C\n")
    f.write(f"  Абсолютный минимум:   {yearly['min_temp'].min():.1f} °C\n\n")

    f.write("СЕЗОННОСТЬ (из monthly_stats)\n")
    f.write(f"  Самый тёплый месяц:   {month_names[int(warmest_m['month'])-1]} ({warmest_m['avg_temp']:.1f}°C)\n")
    f.write(f"  Самый холодный месяц: {month_names[int(coldest_m['month'])-1]} ({coldest_m['avg_temp']:.1f}°C)\n")
    f.write(f"  Годовая амплитуда:    {warmest_m['avg_temp'] - coldest_m['avg_temp']:.1f}°C\n\n")

    f.write("ОСАДКИ (из precipitation_stats)\n")
    f.write(f"  Самый дождливый месяц: {month_names[int(wettest_m['month'])-1]} ({wettest_m['avg_precip']:.2f} мм/день)\n")
    f.write(f"  Среднегодовые осадки:  {precip['total_precip'].mean():.1f} мм/месяц\n\n")

    f.write("ВЫЯВЛЕННЫЕ ЗАКОНОМЕРНОСТИ\n")
    f.write(f"  Тренд потепления за 25 лет: {'да' if trend else 'нет'}\n")
    for _, row in seasons.iterrows():
        f.write(f"  {row['season']:6}: avg={row['avg_temp']:.1f}°C, осадки={row['avg_precip']:.2f} мм/день\n")
    f.write("\n")

    f.write("ГРАФИКИ\n")
    f.write("  1. yearly_temperature.png   — временной ряд по годам\n")
    f.write("  2. monthly_seasonality.png  — сезонность по месяцам\n")
    f.write("  3. monthly_precipitation.png — осадки по месяцам\n")
    f.write("  4. season_stats.png         — температура и осадки по сезонам\n")

print(f"Отчёт: {report_path}")
print("Готово! Все файлы в папке results/")
