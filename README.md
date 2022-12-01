# Distributed-FC
Do we wanna call it football or soccer? 

## ./data
[European Soccer Database (Kaggle)](https://www.kaggle.com/datasets/hugomathien/soccer?resource=download)

#### `Teams`

#### `Team_Attributes`

#### `Matches`


## Notes
I'm using [DBeaver](https://dbeaver.io/) to view and clean db content before exporting to `./data`.
* only using data from the 2015/2016 season due to entry uniformity/completion
* using the team_api_id for unique team identifiers from the original dataset
* some files need to be further cleaned to sort out dupes bc idk how to perform fuzzy searches w SQL
  * `Team_Attributes.csv`
