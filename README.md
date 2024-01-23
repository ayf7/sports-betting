# Project: Sports Betting (NBA)
A large aspects of the sports world is betting: predicting on the outcome of games. Predictions can range from the most high level outcomes, such as who will be the winner of the NBA finals or the superbowl,
to the most specific outcomes, such as how many rebounds a player will get. The goal of this project is to determine to what extent it is possible "beat the book": making a profit off of the odds set by the
bookmakers. As a big NBA fan, I will be focused on NBA moneyline odds: betting on the winner or loser of games.

This project is purely for educational purposes only, and does not endorse gambling.

## Layout
There are three aspects to this project:
* Datascraping: using http requests to obtain stats from official websites, and handling the data using pandas. Datascraping modules can be found in the `scraper` directory.
* Modeling: developing models to fit the training data and predict ongoing games. Found in the `models` directory.
* Predicting and Decision Making: once our model gives a prediction for the final box score (+/-), how does it compare to the odds provided by the bookmaker? Which team should we bet for, and how much? Should we even bet at all? Found in the `main` directory.

## Execution
`main/cmd.py` contains the file that can be executed to run all scripts. Upon execution, you will be prompted a command. The following commands are supported:
* `generate <YYYY-YYYY>` - generates data for a specific season from scratch. You can interrupt and save the file whenever you want - the process will prompt whether to save before exiting.
* `update <YYYY-YYYY>` - resumes data generation for a season. Requires that a season's data has at least been partially generated.
* `aggregate` - combines all generated data into one large CSV.
* `features` - extracts all selected features from the aggregate CSV in `parameters/features.json`
* `daily` - scrapes today's games and features.

`main/predict.py` can be executed to run the model. The current model should be able to be executed out of the box.
