"""
A simple selenium test example written by python
"""

import unittest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


@unittest.skip("Requires external website https://www.oursky.com/")
class TestTemplate(unittest.TestCase):
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
        """Find and click top-right button"""
        try:
            self.driver.get('https://www.oursky.com/')
            el = self.driver.find_element(By.CLASS_NAME, 'btn-header')
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)

    def test_case_2(self):
        """Find and click Learn more button"""
        try:
            self.driver.get('https://www.oursky.com/')
            el = self.driver.find_element(
                By.XPATH, ".//*[@id='tag-line-wrap']/span/a")
            el.click()
        except NoSuchElementException as ex:
            self.fail(ex.msg)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestTemplate)
    unittest.TextTestRunner(verbosity=2).run(suite)
