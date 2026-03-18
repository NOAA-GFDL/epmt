"""
Useful API link for waiting for expected conditions:
https://www.selenium.dev/selenium/docs/api/py/webdriver_support/selenium.webdriver.support.expected_conditions.html
"""
import sys
import unittest
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

try:
    TEST_ADDRESS = str(sys.argv[1])
except IndexError:
    print("No Test Address given. Defaulting to localhost")
    TEST_ADDRESS = 'http://localhost:8050'


class TestTemplate(unittest.TestCase):
    """Include test cases on a given url"""

    def setUp(self):
        """Start web driver"""
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument('--headless')
        options.add_argument("--window-size=1920x1080")
        options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(30)

    def tearDown(self):
        """Stop web driver"""
        self.driver.quit()

    def test_case_1(self):
        """Find and click Recent Jobs tab"""
        try:
            self.driver.get(TEST_ADDRESS)
            # Wait till the server starts and datatable loads
            el = WebDriverWait(self.driver, 60).until(
                EC.presence_of_element_located((By.ID, "table-multicol-sorting")))
            el = self.driver.find_element_by_xpath("//*[@id='tabs']/div[1]/span")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_2(self):
        """Find and Click Models tab"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='tabs']/div[2]/span")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_3(self):
        """Find and Click Raw Data Toggle"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='raw-switch']/div/div/div[2]/button")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_4(self):
        """Find and Click Search Area"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='searchdf']")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_5(self):
        """Find and Click Run Analysis button"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='run-analysis-btn']")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_6(self):
        """Find and Click Create model from jobs button"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='create-newModel-btn']")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_7(self):
        """Find and Click Select All button"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='index-select-all']")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_8(self):
        """Find and Click Date start and end"""
        try:
            self.driver.get(TEST_ADDRESS)
            # Activate date picker dialog
            el = self.driver.find_element_by_xpath("//*[@id='jobs-date-picker']/div/div/div[1]")
            el.click()
            # Click start date
            el = self.driver.find_element_by_xpath(
                "/html/body/div[2]/div/div/div/div/div[2]/div[2]/div/div[2]/div/table/tbody/tr[1]/td[7]")
            el.click()
            # Delay
            self.driver.implicitly_wait(3)
            # Click end date
            el = self.driver.find_element_by_xpath(
                "/html/body/div[2]/div/div/div/div/div[2]/div[2]/div/div[2]/div/table/tbody/tr[2]/td[4]")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_9(self):
        """Find and Click Available test model dropdown"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='react-select-2--value']/div[1]")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_10(self):
        """Find and Click number of jobs to display dropdown"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='react-select-3--value']/div[1]")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_11(self):
        """Find and Click issue tracker"""
        try:
            self.driver.get(TEST_ADDRESS)
            el = self.driver.find_element_by_xpath("//*[@id='version']/a")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)
    # Test Models tab

    def test_case_12(self):
        """Find and click Models tab, test all buttons"""
        try:
            self.driver.get(TEST_ADDRESS)
            # Find and click Models tab
            el = self.driver.find_element_by_xpath("//*[@id='tabs']/div[2]/span")
            el.click()
            # Find and Click Toggle Model Status
            el = self.driver.find_element_by_xpath("//*[@id='toggle-Model-btn']")
            el.click()
            # Find and Click Edit Reference Model
            el = self.driver.find_element_by_xpath("//*[@id='edit-Model-btn']")
            el.click()
            # Find and Click Delete Model button
            el = self.driver.find_element_by_xpath("//*[@id='delete-Model-btn']")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_13(self):
        """Handle unselections on search entry"""
        try:
            self.driver.get(TEST_ADDRESS + "?case=13")

            # We should wait till rows are available before clicking things
            el = WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "//tr[1]/td/input")))
            el = self.driver.find_element_by_xpath("//tr[30]/td/input")
            state = el.is_selected()
            # Select all
            el = self.driver.find_element_by_xpath("//*[@id='index-select-all']")
            el.click()
            # Delay for callback to select everyone
            from time import sleep
            sleep(5)
            # Check if last row(#30) is selected
            el = self.driver.find_element_by_xpath("//tr[30]/td/input")
            newstate = el.is_selected()
            if newstate == state:
                self.fail("Selection state didn't toggle on newstate:{} oldstate:{}".format(newstate, state))
            state = newstate
            # Type 007 into search box to fire text callback
            input_box = self.driver.find_element_by_xpath("//*[@id='searchdf']")
            input_box.send_keys('00')
            input_box.send_keys('7')
            # Jobid 1234007 checkbox
            WebDriverWait(self.driver, 200).until(
                EC.element_located_selection_state_to_be((By.XPATH, "//tr[1]/td[1]/input"), False))
            el = self.driver.find_element_by_xpath("//tr[1]/td[1]/input")
            newstate = el.is_selected()
            if newstate == state:
                self.fail("Selections were not unselected newstate:{} oldstate:{}".format(newstate, state))

        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_14(self):
        """Handle unselections on date picker query"""
        try:
            self.driver.get(TEST_ADDRESS + "?case=14")

            # Wait for table rows to be present
            # //*[@id='table-multicol-sorting']/div[2]/div/div[2]/div[2]/table/tbody/tr[30]/td[1]
            WebDriverWait(
                self.driver, 200).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[@id='table-multicol-sorting']/div[2]/div/div[2]/div[2]/table/tbody/tr[30]/td[1]")))
            el = self.driver.find_element_by_xpath(
                "//*[@id='table-multicol-sorting']/div[2]/div/div[2]/div[2]/table/tbody/tr[30]/td[1]")
            state = el.is_selected()
            # Select all
            el = self.driver.find_element_by_xpath("//*[@id='index-select-all']")
            el.click()
            # Delay for callback to select everyone
            # We should wait till rows are available before clicking things
            WebDriverWait(self.driver, 200).until(
                EC.element_located_selection_state_to_be((By.XPATH, "//tr[30]/td/input"), True))
            # Check if last row(#30) is selected
            el = self.driver.find_element_by_xpath("//tr[30]/td/input")
            newstate = el.is_selected()
            if newstate == state:
                self.fail("Selection state didn't toggle on newstate:{} oldstate:{}".format(newstate, state))
            state = newstate
            # enter a full date selection
            # Activate date picker dialog
            el = self.driver.find_element_by_xpath("//*[@id='jobs-date-picker']/div/div/div[1]")
            el.click()
            # Click start date
            el = self.driver.find_element_by_xpath(
                "/html/body/div[2]/div/div/div/div/div[2]/div[2]/div/div[2]/div/table/tbody/tr[1]/td[7]")
            el.click()

            # Click end date
            el = self.driver.find_element_by_xpath(
                "/html/body/div[2]/div/div/div/div/div[2]/div[2]/div/div[2]/div/table/tbody/tr[2]/td[4]")
            el.click()

            # We should wait till rows are available before clicking things
            WebDriverWait(self.driver, 200).until(
                EC.element_located_selection_state_to_be((By.XPATH, "//tr[1]/td/input"), False))
            # Jobid 1234007 checkbox
            el = self.driver.find_element_by_xpath("//tr[1]/td/input")
            newstate = el.is_selected()
            if newstate == state:
                self.fail("Selections were not unselected newstate:{} oldstate:{}".format(newstate, state))

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
    # suite.addTest(TestTemplate("test_case_13"))

    # Load entire template of cases
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTemplate)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
