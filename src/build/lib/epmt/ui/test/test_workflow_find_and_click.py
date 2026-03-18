"""
A simple selenium test example written by python
"""
import sys
import unittest
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

try:
    host = str(sys.argv[1])
except IndexError:
    print("No Test Address given. Defaulting to localhost")
    host = 'http://localhost:8050'


class TestTemplate(unittest.TestCase):
    """Include test cases on a given url"""

    def setUp(self):
        """Start web driver"""
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(15)

    def tearDown(self):
        """Stop web driver"""
        self.driver.quit()

    def test_case_1(self):
        """Load Page, read title"""
        try:
            self.driver.get(host + "/graph/bar/?expname=ESM1_historical&metric=duration,cpu_time")
            WebDriverWait(self.driver, 200).until(
                EC.presence_of_element_located((By.XPATH, "//*[@id='page-content']/div/div[1]/div/div[1]/div/h5")))
            el = self.driver.find_element_by_xpath("//*[@id='page-content']/div/div[1]/div/div[1]/div/h5").text
            self.assertEqual(el, "Experiment Performance Management Tool")
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_2(self):
        """See if experiment graph was made by id"""
        try:
            self.driver.get(host + "/graph/bar/?expname=ESM1_historical&metric=duration,cpu_time")
            el = self.driver.find_element_by_id("bargraph")
            # self.assertEqual(el, "Experiment Performance Management Tool")
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_3(self):
        """See if component graph was made by id"""
        try:
            self.driver.get(
                host +
                "/graph/bar?metric=duration,cpu_time&expname=ESM1_historical&exp_component=ocean_annual_rho2_1x1deg")
            el = self.driver.find_element_by_id("bargraph")
            # self.assertEqual(el, "Experiment Performance Management Tool")
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_4(self):
        """See if job graph was made by id"""
        try:
            self.driver.get(
                host +
                "/graph/bar?metric=duration,cpu_time&expname=ESM1_historical&exp_component=land_annual_rho2_1x1deg&jobs=1234043&op=op")
            el = self.driver.find_element_by_id("graph-area")
            # self.assertEqual(el, "Experiment Performance Management Tool")
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    # def test_case_2(self):
    #     """Find and click Learn more button"""
    #     try:
    #         self.driver.get('https://www.oursky.com/')
    #         el = self.driver.find_element_by_xpath(".//*[@id='tag-line-wrap']/span/a")
    #         el.click()
    #     except NoSuchElementException as ex:
    #         self.fail(ex.msg)
if __name__ == '__main__':
    suite = unittest.TestSuite()
    #
    # Uncomment to test a single case
    # suite.addTest(TestTemplate("test_case_1"))

    # Load entire template of cases
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTemplate)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
