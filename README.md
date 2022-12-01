# Distributed-FC
Football or soccer? 

## ./data
[European Soccer Database (Kaggle)](https://www.kaggle.com/datasets/hugomathien/soccer?resource=download)

For consistency:
 * team ids refer to `team_api_id` in the dataset
 * `bu` -> build up
 * `cc` -> chance creation
 * `d` -> defense

### `Teams`
`Id`: Int <br/>
`name`: String

### `Team_Attributes`
`Id`: Int <br/>
`date`: Date() <br/>
`buSpeed`: String <br/>
`buDribbling`: String <br/>
`buPassing`: String <br/>
`buPositioning`: String <br/>
`ccPassing`: String <br/>
`ccCrossing`: String <br/>
`ccShooting`: String <br/>
`ccPositioning`: String <br/>
`dPressure`: String <br/>
`dAggression`: String <br/>
`dWidth`: String <br/>
`dLine`: String <br/>

### `Matches`
`Id`: Int <br/>
`homeTeamID`: Int <br/>
`awayTeamID`: Int <br/>
`homeGoals`: Int <br/>
`awayGoals`: Int


## Notes
I'm using [DBeaver](https://dbeaver.io/) to view and clean db content before exporting to `./data`.
* only using data from the 2015/2016 season due to entry uniformity/completion
* some files need to be further cleaned to sort out dupes bc idk how to perform fuzzy searches w SQL
  * `Team_Attributes.csv`: Teams have multiple entries from different seasons, we only want entries dated 2015
  
`Team_Attributes` stats have a variety of options
* ie. x`Passing` can be normal, risky, or safe
* I haven't collected all the possible descriptions for each category yet
