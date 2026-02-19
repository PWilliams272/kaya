import pandas as pd
from . import db_manager


class ClimbingDataAnalyzer:
    """Handle data processing and analysis tasks.
    """
    def __init__(
        self,
        use_aws: bool = False,
        read_data: bool = False,
        data: pd.DataFrame = None,
    ) -> None:
        """Initialize the Analysis class

        Args:
            use_aws (bool): Flag indicating whether to use AWS or your local
                db for data reading.
            read_data (bool): Flag indicating whether to read data on
                initialization.
            data (pd.DataFrame): Optional pre-loaded data. Only used if
                read_data is False.

        """
        self.use_aws = use_aws
        if read_data:
            self.data = self.read_data().sort_values('date')
        else:
            self.data = data.sort_values('date')

    def read_data(
        self,
        use_aws: bool = None
    ) -> pd.DataFrame:
        """Read data from a source (local or AWS).

        Calls kaya.db_manager.read_table() to fetch the sends table from
        AWS or your local db.

        Args:
            use_aws (bool): Flag indicating whether to use AWS or your local
                db for data reading.

        Returns:
            pd.DataFrame: The data read from the source.
        """
        if use_aws is None:
            use_aws = self.use_aws
        return db_manager.read_table('sends', use_aws=use_aws)