import os

import pandas as pd

from flask_wtf import FlaskForm
from wtforms import SubmitField, MultipleFileField, StringField, SelectField
from flask import Flask, render_template, request, url_for, redirect
from flask_bootstrap import Bootstrap

app = Flask(__name__)
app.secret_key = os.urandom(27)

fr = None


class GeneralForm(FlaskForm):
    files = MultipleFileField(label='Upload File')
    path = StringField(label='Path',)
    submit = SubmitField('Submit')


class ViewerForm(FlaskForm):
    column = SelectField(label='Column')
    search = StringField(label='Search')
    submit = SubmitField('Submit')


@app.route('/', methods=['GET', 'POST'])
def index():
    global fr
    form = GeneralForm(request.form)
    if request.method == 'POST':
        string = form.path.data
        files = request.files.getlist(form.files.name)
        for name, file in zip(form.files.data, files):
            fr = FileReader(name, file.stream.read(), is_file=False)
            break
        if not fr and string:
            fr = FileReader(os.path.basename(string), string)
        if fr:
            return redirect(url_for('viewer'))
    return render_template('index.html', form=form)


@app.route('/viewer', methods=['GET', 'POST'])
def viewer():
    global fr
    form = ViewerForm(request.form)
    form.column.choices = [(c, c) for c in fr.columns]
    if request.method == 'POST':  # search or next
        label = form.column.data
        search = form.search.data
        return render_template('viewer.html', form=form, data=fr.search(label, search), messages=[fr.error])
    return render_template('viewer.html', form=form, data=fr.reset(), messages=[fr.error])


class FileReader:

    def __init__(self, filename: str, data, is_file=True):
        self.data = None
        self.index = -1
        self.subquery_indices = None
        self.subquery_index = 0
        fn = filename.strip('"\'')
        if isinstance(data, str):
            data = data.strip('"\'')
        if fn.endswith('.csv'):
            try:
                self.data = pd.read_csv(data)
            except UnicodeDecodeError:
                self.data = pd.read_csv(data, encoding='cp1252')
        elif fn.endswith('.sas7bdat'):
            self.data = pd.read_sas(data, encoding='cp1252')
        elif os.path.isdir(data):
            self.data = self._read_directory(data)
        else:
            raise NotImplementedError(f'Extension not recognized or not implemented: {data}')
        self.size = self.data.shape[0]
        self.columns = self.data.columns
        self._error = None
        self._level = 'warning'  # error level

    def _read_directory(self, directory):
        records = []
        for filename in os.listdir(directory):
            records.append(self._read_file(filename, os.path.join(directory, filename)))
        return pd.DataFrame(records)

    def _read_file(self, filename, fp):
        if fp.endswith('.txt'):
            with open(fp) as fh:
                text = fh.read()
            return {'filename': filename, 'text': text}
        else:
            raise NotImplementedError(f'Extension not recognized or not implemented: {filename}')

    @property
    def error(self):
        val = ''
        if self._error:
            val = self._error
            self._error = None
        return {'level': self._level, 'message': val}

    @error.setter
    def error(self, val):
        if isinstance(val, tuple):
            self._error = val[0]
            self._level = val[1]
            if self._level not in {'success', 'info', 'warning', 'danger'}:
                self._level = 'warning'
        else:
            self._error = val
            self._level = 'warning'

    def __bool__(self):
        return self.data is not None

    def _move(self, idx):
        if self.subquery_index:
            self.subquery_index = (self.subquery_index + idx) % len(self.subquery_indices)
            return self.get_form_data_for_row(self.subquery_indices[self.subquery_index])
        else:
            self.index = (self.index + idx) % self.size
            return self.get_form_data_for_row(self.index)

    def reset(self):
        self.subquery_indices = None
        return self._move(0)

    def next(self):
        return self._move(1)

    def prev(self):
        return self._move(-1)

    def search(self, label, value):
        try:
            self.subquery_indices = list(self.data[self.data[label] == value].index)
        except Exception as e:
            self.error = f'Fatal error in search: {e}'
        if len(self.subquery_indices) == 0:
            self.error = 'No matches'
            return self._move(0)
        self.subquery_index = 0
        return self.get_form_data_for_row(self.subquery_indices[self.subquery_index])

    def get_form_data_for_row(self, idx):
        results = []
        for col in self.data.columns:
            dat = self.data.loc[idx, col]
            if dat is None:
                results.append({'column': col, 'label': 'text', 'value': 'None'})
            elif isinstance(dat, (float, int)):
                results.append({'column': col, 'label': 'text', 'value': f'{dat}'})
            elif len(str(dat)) > 20:
                results.append({'column': col, 'label': 'textarea', 'value': f'{dat}'})
            else:
                results.append({'column': col, 'label': 'text', 'value': f'{dat}'})
        return results


bootstrap = Bootstrap(app)
if __name__ == '__main__':
    app.run(debug=True)
