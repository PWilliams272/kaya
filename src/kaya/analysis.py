import pandas as pd
from . import db_manager
from gender_guesser.detector import Detector


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
            self.data = self.read_data()
        else:
            self.data = data
        
        if self.data is not None:
            self.clean_data()
            self.process_users()

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

    @staticmethod
    def inches_to_ft_inches(
        inches: float
    ) -> str:
        """Convert inches to format ft'in"

        Args:
            inches (float): The height in inches.
        """
        if pd.isna(inches):
            return None
        return f"{int(inches//12)}\' {int(inches%12)}\""

    @staticmethod
    def boulder_grade_num_to_str(
        grade: float
    ) -> str:
        if grade == -1:
            return 'vB'
        else:
            return f"v{grade}"

    def clean_data(
        self
    ):
        """Clean and standardize the data
        """
        self.data = self.data.sort_values('date')
        self.data['date'] = pd.to_datetime(self.data['date']).dt.date

        # Convert grades to numbers
        GRADE_TO_NUM = {f'v{i}': i for i in range(18)}
        GRADE_TO_NUM.update({'vB': -1, 'vIntro': -1, 'v?': None})
        GRADE_TO_NUM.update({f'5.{i}': i for i in range(1, 10)})
        GRADE_TO_NUM.update({f'5.{i}a': i + 0.0 for i in range(10, 16)})
        GRADE_TO_NUM.update({f'5.{i}b': i + 0.25 for i in range(10, 16)})
        GRADE_TO_NUM.update({f'5.{i}c': i + 0.5 for i in range(10, 16)})
        GRADE_TO_NUM.update({f'5.{i}d': i + 0.75 for i in range(10, 16)})
        GRADE_TO_NUM.update({'5.Intro': 5, '5.Hard': None, '5.?': None})
        self.data['grade_num'] = self.data['grade'].map(GRADE_TO_NUM)

    def process_users(
        self
    ):
        """Build user profile DataFrame
        
        """
        users = self.data.groupby('user_id', as_index=False).agg(
            height = ('height', 'first'),
            ape_index = ('ape_index', 'first'),
            first_name = ('first_name', 'first'),
            n_sends = ('send_id', 'count'),
            n_dates = ('date', 'nunique'),
            first_send = ('date', 'min'),
            last_send = ('date', 'max')
        )

        # Use gender guesser to infer gender based on name
        gender_detector = Detector(case_sensitive=False)
        users['gender'] = users['first_name'].apply(
            gender_detector.get_gender
        )
        users = users.drop(columns=['first_name'])

        # Convert to inches
        users['height'] /= 2.54
        users['ape_index'] /= 2.54

        # Get human-readable heights
        users['height_hr'] = users['height'].apply(
            self.inches_to_ft_inches
        )

        # Get usage frequency
        users['sends_per_date'] = users['n_sends'] / users['n_dates']
        timeframe = (users['last_send'] - users['first_send']).apply(lambda x: x.days) + 1
        users['sends_per_month'] = users['n_sends'] / timeframe / 30.

        # TODO: Segment users based on usage patterns
        # - Super-users: Likely log most or every send. High sends per date,
        #   high sends per month, and a wide range of grades.
        # - One-time users: Created an account, logged a few climbs, stopped
        # - Limit users: Only log climbs near their limit. Lots of logs, but
        #   very small range of grades.

        self.users = users