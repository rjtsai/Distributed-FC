# Distributed-FC
Football or soccer? 

## Spec
`project sdk`: `1.8.0` <br />
`scala`: `2.11.8` <br />
`sbt`: `1.3.12`

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
* Idk how to integrate sql worksheets from DBeaver into a gh repo, I can share the sql code I'm using separately
* only using data from the 2015/2016 season due to entry uniformity/completion
* some files need to be further cleaned to sort out dupes 
  * `Team_Attributes.csv`: Some teams that appear in `Teams` do not have data entries in 2015 in `Team_Attributes`
  
`Team_Attributes` stats have a variety of options
* ie. x`Passing` can be normal, risky, or safe
* I haven't collected all the possible descriptions for each category yet
