# PolishPollution

This is a data engineering project that aims for creating a **Streamlit** app, preceded by utilizing Snowflake tools like **Dynamic Tables and SnowPipes**. 

Project is focused on air pollution, weather conditions and population data for cities over 100k population in Poland.

It could obviously be used for larger list of locations - once higher API subscription is provided.

The population limit results from API rates that are allowed with free subscriptions. This project was originally meant to utilize OpenAQ API with OpenMeteo or VisualCrossing API, but since the **OpenAQ** measurements and latest measurements endpoints have been facing internal server errors since a long time and do not provide current results, i had to find another data source:

**OpenWeatherMap AirPollution and Weather** APIs. To limit the allowable rates, I've found a dataset from **Opendatasoft** - detailed informations on cities with population > 1000, worldwide. Filtered that to my needs and used in this project. The transformed sample, with Polish-only citites and population > 100k is in this repo.

To read more about this project, view some examples and browse a few images, I encourage You to check this PDF Overview that i created:

[PDF Overview](https://github.com/halikowski/PolishPollution/blob/main/PolishPollution-ProjectOverview.pdf)
