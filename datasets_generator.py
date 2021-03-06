import numpy as np
import pandas as pd
import random
import re
import logging
import os

logging.basicConfig(level=logging.NOTSET)


def generate_customer_profiles_table(n_customers, random_state=0):
    """Generate customer table"""

    np.random.seed(random_state)

    customer_id_properties = []

    # Generate customer properties from random distributions 
    for customer_id in range(n_customers):
        x_customer_id = np.random.uniform(0, 100)
        y_customer_id = np.random.uniform(0, 100)

        mean_amount = np.random.uniform(5, 100)  # Arbitrary (but sensible) value
        std_amount = mean_amount / 2  # Arbitrary (but sensible) value

        mean_nb_tx_per_day = np.random.uniform(0, 4)  # Arbitrary (but sensible) value

        customer_id_properties.append([customer_id,
                                       x_customer_id, y_customer_id,
                                       mean_amount, std_amount,
                                       mean_nb_tx_per_day])

    customer_profiles_table = pd.DataFrame(customer_id_properties, columns=['CUSTOMER_ID',
                                                                            'x_customer_id', 'y_customer_id',
                                                                            'mean_amount', 'std_amount',
                                                                            'mean_nb_tx_per_day'])

    return customer_profiles_table


def generate_terminal_profiles_table(n_terminals, random_state=0):
    """Generate Terminal table"""
    np.random.seed(random_state)

    terminal_id_properties = []

    # Generate terminal properties from random distributions 
    for terminal_id in range(n_terminals):
        x_terminal_id = np.random.uniform(0, 100)
        y_terminal_id = np.random.uniform(0, 100)

        terminal_id_properties.append([terminal_id,
                                       x_terminal_id, y_terminal_id])

    terminal_profiles_table = pd.DataFrame(terminal_id_properties, columns=['TERMINAL_ID',
                                                                            'x_terminal_id', 'y_terminal_id'])

    return terminal_profiles_table


def get_list_terminals_within_radius(customer_profile, x_y_terminals, r):
    """Association of customer profiles to terminals"""

    # Use numpy arrays in the following to speed up computations

    # Location (x,y) of customer as numpy array
    x_y_customer = customer_profile[['x_customer_id', 'y_customer_id']].values.astype(float)

    # Squared difference in coordinates between customer and terminal locations
    squared_diff_x_y = np.square(x_y_customer - x_y_terminals)

    # Sum along rows and compute suared root to get distance
    dist_x_y = np.sqrt(np.sum(squared_diff_x_y, axis=1))

    # Get the indices of terminals which are at a distance less than r
    available_terminals = list(np.where(dist_x_y < r)[0])

    # Return the list of terminal IDs
    return available_terminals


def generate_transactions_table(customer_profile, start_date, nb_days):
    """Generate associations table"""
    customer_transactions = []

    random.seed(customer_profile.CUSTOMER_ID)
    np.random.seed(customer_profile.CUSTOMER_ID)

    # For all days
    for day in range(nb_days):

        # Random number of transactions for that day 
        nb_tx = np.random.poisson(customer_profile.mean_nb_tx_per_day)

        # If nb_tx positive, let us generate transactions
        if nb_tx > 0:

            for tx in range(nb_tx):

                # Time of transaction: Around noon, std 20000 seconds. This choice aims at simulating the fact that 
                # most transactions occur during the day.
                time_tx = int(np.random.normal(86400 / 2, 20000))

                # If transaction time between 0 and 86400, let us keep it, otherwise, let us discard it
                if (time_tx > 0) and (time_tx < 86400):

                    # Amount is drawn from a normal distribution  
                    amount = np.random.normal(customer_profile.mean_amount, customer_profile.std_amount)

                    # If amount negative, draw from a uniform distribution
                    if amount < 0:
                        amount = np.random.uniform(0, customer_profile.mean_amount * 2)

                    amount = np.round(amount, decimals=2)

                    if len(customer_profile.available_terminals) > 0:
                        terminal_id = random.choice(customer_profile.available_terminals)

                        customer_transactions.append([time_tx + day * 86400, day,
                                                      customer_profile.CUSTOMER_ID,
                                                      terminal_id, amount])

    customer_transactions = pd.DataFrame(customer_transactions,
                                         columns=['TX_TIME_SECONDS', 'TX_TIME_DAYS', 'CUSTOMER_ID', 'TERMINAL_ID',
                                                  'TX_AMOUNT'])

    if len(customer_transactions) > 0:
        customer_transactions['TX_DATETIME'] = pd.to_datetime(customer_transactions["TX_TIME_SECONDS"], unit='s',
                                                              origin=start_date)
        customer_transactions = customer_transactions[
            ['TX_DATETIME', 'CUSTOMER_ID', 'TERMINAL_ID', 'TX_AMOUNT', 'TX_TIME_SECONDS', 'TX_TIME_DAYS']]

    return customer_transactions


def add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df):
    """Fraud scenarios generation"""
    logging.info("Adding frauds")

    # By default, all transactions are genuine
    transactions_df['TX_FRAUD'] = 0
    transactions_df['TX_FRAUD_SCENARIO'] = 0

    # Scenario 1
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD'] = 1
    transactions_df.loc[transactions_df.TX_AMOUNT > 220, 'TX_FRAUD_SCENARIO'] = 1
    nb_frauds_scenario_1 = transactions_df.TX_FRAUD.sum()

    # Scenario 2
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_terminals = terminal_profiles_table.TERMINAL_ID.sample(n=2, random_state=day)

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day + 28) &
                                                   (transactions_df.TERMINAL_ID.isin(compromised_terminals))]

        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD'] = 1
        transactions_df.loc[compromised_transactions.index, 'TX_FRAUD_SCENARIO'] = 2

    nb_frauds_scenario_2 = transactions_df.TX_FRAUD.sum() - nb_frauds_scenario_1

    # Scenario 3
    for day in range(transactions_df.TX_TIME_DAYS.max()):
        compromised_customers = customer_profiles_table.CUSTOMER_ID.sample(n=3, random_state=day).values

        compromised_transactions = transactions_df[(transactions_df.TX_TIME_DAYS >= day) &
                                                   (transactions_df.TX_TIME_DAYS < day + 14) &
                                                   (transactions_df.CUSTOMER_ID.isin(compromised_customers))]

        nb_compromised_transactions = len(compromised_transactions)

        random.seed(day)
        index_fauds = random.sample(list(compromised_transactions.index.values),
                                    k=int(nb_compromised_transactions / 3))

        transactions_df.loc[index_fauds, 'TX_AMOUNT'] = transactions_df.loc[index_fauds, 'TX_AMOUNT'] * 5
        transactions_df.loc[index_fauds, 'TX_FRAUD'] = 1
        transactions_df.loc[index_fauds, 'TX_FRAUD_SCENARIO'] = 3

    nb_frauds_scenario_3 = transactions_df.TX_FRAUD.sum() - nb_frauds_scenario_2 - nb_frauds_scenario_1

    logging.info("Frauds added")

    return transactions_df


def generate_dataset(start_date, nb_days):
    """Generate larger datasets"""
    logging.info("Generating datasets for date {} and {} days of transactions".format(start_date, nb_days))

    customer_profiles_table = generate_customer_profiles_table(5000, random_state=0)

    terminal_profiles_table = generate_terminal_profiles_table(10000, random_state=1)

    x_y_terminals = terminal_profiles_table[['x_terminal_id', 'y_terminal_id']].values.astype(float)
    customer_profiles_table['available_terminals'] = customer_profiles_table.apply(
        lambda x: get_list_terminals_within_radius(x, x_y_terminals=x_y_terminals, r=5), axis=1)
    # With Pandarallel
    # customer_profiles_table['available_terminals'] = customer_profiles_table.parallel_apply(lambda x : get_list_closest_terminals(x, x_y_terminals=x_y_terminals, r=r), axis=1)
    customer_profiles_table['nb_terminals'] = customer_profiles_table.available_terminals.apply(len)

    transactions_df = customer_profiles_table.groupby('CUSTOMER_ID').apply(
        lambda x: generate_transactions_table(x.iloc[0], start_date, nb_days=nb_days)).reset_index(drop=True)
    # With Pandarallel
    # transactions_df=customer_profiles_table.groupby('CUSTOMER_ID').parallel_apply(lambda x : generate_transactions_table(x.iloc[0], nb_days=nb_days)).reset_index(drop=True)

    # Sort transactions chronologically
    transactions_df = transactions_df.sort_values('TX_DATETIME')
    # Reset indices, starting from 0
    transactions_df.reset_index(inplace=True, drop=True)
    transactions_df.reset_index(inplace=True)
    # TRANSACTION_ID are the dataframe indices, starting from 0
    transactions_df.rename(columns={'index': 'TRANSACTION_ID'}, inplace=True)
    # TRANSACTION_ID are the combination of TX_DATETIME and TERMINAL_ID
    transactions_df['TRANSACTION_ID'] = transactions_df.apply(
        lambda row: "".join(re.findall('\d+', str(row.TX_DATETIME))) + str(row.TERMINAL_ID), axis=1)

    logging.info("Dataset generated")

    return customer_profiles_table, terminal_profiles_table, transactions_df


class DatasetsGenerator:
    def __init__(self, start_date, nb_days):
        self.start_date = start_date
        self.nb_days = nb_days

    def run(self):
        (customer_profiles_table, terminal_profiles_table, transactions_df) = \
            generate_dataset(self.start_date, self.nb_days)

        transactions_df = add_frauds(customer_profiles_table, terminal_profiles_table, transactions_df)

        self.save_datasets(customer_profiles_table, terminal_profiles_table, transactions_df)

    def save_datasets(self, customer_profiles_table, terminal_profiles_table, transactions_df):
        path = "./datasets"

        if not os.path.exists(path):
            os.makedirs(path)

        files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

        transactions_file = "{}/transactions_{}.json".format(path, self.start_date)

        if transactions_file not in files:
            transactions_df.to_json(transactions_file, orient="records")

        customers_file = "{}/customers.json".format(path)

        if customers_file not in files:
            customer_profiles_table.to_json(customers_file, orient="records")

        terminals_file = "{}/terminals.json".format(path)

        if terminals_file not in files:
            terminal_profiles_table.to_json(terminals_file, orient="records")

        logging.info("Datasets saved")
