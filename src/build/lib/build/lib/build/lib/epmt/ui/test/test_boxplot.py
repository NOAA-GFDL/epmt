"""
A simple selenium test example written by python
"""

import unittest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


@unittest.skip("Requires running server at http://172.17.0.1:8050")
class Boxplot_tests(unittest.TestCase):
    """Include test cases on a given url"""

    def setUp(self):
        """Start web driver"""
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(10)

    def tearDown(self):
        """Stop web driver"""
        self.driver.quit()

    def test_case_1(self):
        """Check title"""
        try:
            self.driver.get(
                'http://172.17.0.1:8050/graph/boxplot/test_model/?jobs=685016')

            # Depending on model size and number of jobs
            # this can take over 30 seconds
            self.driver.implicitly_wait(40)
            el = self.driver.find_element(By.CLASS_NAME, 'gtitle')
            print("Title found {}".format(el.text))
            self.assertTrue(
                'Mean normalized cpu_time Per Op: Jobs(685016)  versus Model(test_model)' == el.text)
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_2(self):
        """normalize:off metric:duration"""
        try:
            self.driver.get(
                'http://172.17.0.1:8050/graph/boxplot/test_model/?jobs=685016&normalize=False&metric=duration')

            # Depending on model size and number of jobs
            # this can take over 30 seconds
            self.driver.implicitly_wait(40)
            # Check Title
            el = self.driver.find_element(By.CLASS_NAME, 'gtitle')
            print("Title found {}".format(el.text))
            self.assertTrue(
                'duration Per Op: Jobs(685016)  versus Model(test_model)' == el.text)
            # Check x axis
            el = self.driver.find_element(By.CLASS_NAME, 'g-xtitle')
            print("x axis found {}".format(el.text))
            self.assertTrue('duration' == el.text)
            # Check y axis
            el = self.driver.find_element(By.CLASS_NAME, 'g-ytitle')
            print("y axis found {}".format(el.text))
            self.assertTrue('op' == el.text)
        except NoSuchElementException as ex:
            self.fail(ex.msg)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(Boxplot_tests)
    unittest.TextTestRunner(verbosity=2).run(suite)
