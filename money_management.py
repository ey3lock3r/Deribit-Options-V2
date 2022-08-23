


    def calculate_imargin(self):
        # Futures >>
        # The initial margin starts with 2.0% (50x leverage trading) and linearly increases by 0.5% per 100 BTC increase in position size.
        # Initial margin = 2% + (Position Size in BTC) * 0.005%

        # Options >>
        # The initial margin is calculated as the amount of BTC that will be reserved to open a position.
        # Long call/put: None
        
        # Short call: Maximum (0.15 - OTM Amount/Underlying Mark Price, 0.1) + Mark Price of the Option
        # Short put : Maximum (Maximum (0.15 - OTM Amount/Underlying Mark Price, 0.1) + Mark Price of the Option, Maintenance Margin)

        pass

    def calculate_mmargin(self):
        # Futures >>
        # The maintenance margin starts with 1% and linearly increases by 0.5% per 100 BTC increase in position size.
        # When the account margin balance is lower than the maintenance margin, positions in the account will be incrementally reduced to keep the maintenance margin 
        # lower than the equity in the account. Maintenance margin requirements can be changed without prior notice if market circumstances demand such action.
        # Maintenance Margin= 1% + (PositionSize in BTC) * 0.005%

        # Options >>
        # The maintenance margin is calculated as the amount of BTC that will be reserved to maintain a position.
        # Long call/put: None

        # Short call: 0.075 + Mark Price of the Option
        # Short put : Maximum (0.075, 0.075 * Mark Price of the Option) + Mark Price of the Option

        pass