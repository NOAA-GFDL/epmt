"""Unit tests for the epmt notebook command."""

import unittest
from unittest.mock import patch, MagicMock


class TestEpmtNotebook(unittest.TestCase):
    """Tests for epmt_notebook function."""

    @patch('epmt.epmt_cmd_notebook.LabApp', create=True)
    def test_notebook_default_launches_jupyterlab(self, mock_labapp):
        """Test that calling epmt_notebook with no args launches JupyterLab."""
        mock_labapp_module = MagicMock()
        mock_labapp_module.LabApp = mock_labapp

        with patch.dict('sys.modules', {'jupyterlab': mock_labapp_module,
                                        'jupyterlab.labapp': mock_labapp_module}):
            from epmt.epmt_cmd_notebook import epmt_notebook
            result = epmt_notebook([])

        self.assertTrue(result)
        mock_labapp.launch_instance.assert_called_once()
        call_argv = mock_labapp.launch_instance.call_args.kwargs.get('argv', [])
        # Verify ServerApp.root_dir is passed
        self.assertTrue(any('--ServerApp.root_dir=' in arg for arg in call_argv))

    @patch('epmt.epmt_cmd_notebook.LabApp', create=True)
    def test_notebook_passes_extra_args(self, mock_labapp):
        """Test that extra arguments are forwarded to JupyterLab."""
        mock_labapp_module = MagicMock()
        mock_labapp_module.LabApp = mock_labapp

        with patch.dict('sys.modules', {'jupyterlab': mock_labapp_module,
                                        'jupyterlab.labapp': mock_labapp_module}):
            from epmt.epmt_cmd_notebook import epmt_notebook
            result = epmt_notebook(['--ip', '0.0.0.0'])

        self.assertTrue(result)
        mock_labapp.launch_instance.assert_called_once()
        call_argv = mock_labapp.launch_instance.call_args[1].get('argv') or \
                    mock_labapp.launch_instance.call_args[0][0]
        self.assertIn('--ip', call_argv)
        self.assertIn('0.0.0.0', call_argv)

    @patch('epmt.epmt_cmd_notebook.start_ipython', create=True)
    def test_notebook_kernel_mode_starts_ipython(self, mock_start_ipython):
        """Test that 'kernel' arg starts IPython kernel."""
        mock_ipython_module = MagicMock()
        mock_ipython_module.start_ipython = mock_start_ipython

        with patch.dict('sys.modules', {'IPython': mock_ipython_module}):
            from epmt.epmt_cmd_notebook import epmt_notebook
            result = epmt_notebook(['kernel'])

        self.assertTrue(result)
        mock_start_ipython.assert_called_once()
        call_argv = mock_start_ipython.call_args[1].get('argv') or \
                    mock_start_ipython.call_args[0][0]
        self.assertIn('kernel', call_argv)

    def test_notebook_kernel_mode_missing_ipython(self):
        """Test graceful failure when IPython is not available in kernel mode."""
        with patch.dict('sys.modules', {'IPython': None}):
            from epmt.epmt_cmd_notebook import epmt_notebook
            with patch('builtins.__import__', side_effect=_import_error_for('IPython')):
                result = epmt_notebook(['kernel'])
        self.assertFalse(result)

    def test_notebook_missing_jupyterlab(self):
        """Test graceful failure when jupyterlab is not available."""
        with patch.dict('sys.modules', {'jupyterlab': None, 'jupyterlab.labapp': None}):
            from epmt.epmt_cmd_notebook import epmt_notebook
            with patch('builtins.__import__', side_effect=_import_error_for('jupyterlab')):
                result = epmt_notebook([])
        self.assertFalse(result)


def _import_error_for(module_name):
    """Helper to create an __import__ side_effect that raises ImportError for a specific module."""
    import builtins
    original_import = builtins.__import__

    def _side_effect(name, *args, **kwargs):
        if name == module_name or name.startswith(module_name + '.'):
            raise ImportError(f"No module named '{module_name}'")
        return original_import(name, *args, **kwargs)

    return _side_effect


if __name__ == '__main__':
    unittest.main()
