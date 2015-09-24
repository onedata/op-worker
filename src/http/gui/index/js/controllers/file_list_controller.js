App.FileListController = Ember.ArrayController.extend({
    selectedCount: function () {
        return this.filterBy('selected').length;
    }.property('@each.selected'),

    areAllSelected: function (key, value) {
        if (value === undefined) {
            return !!this.get('length') && this.isEvery('selected', true);
        } else {
            this.setEach('selected', value);
            return value;
        }
    }.property('@each.selected'),

    isAnySelected: function (key, value) {
        if (value === undefined) {
            return !!this.get('length') && this.isAny('selected', true);
        } else {
            return value;
        }
    }.property('@each.selected'),

    isNoneSelected: Ember.computed.not('isAnySelected'),

    isOneSelected: function (key, value) {
        if (value === undefined) {
            return this.get('selectedCount') == 1;
        } else {
            return value;
        }
    }.property('@each.selected'),

    inNotOneSelected: Ember.computed.not('isOneSelected'),

    currentFile: function (key, value) {
        console.log('key ' + key);
        console.log('value ' + value);
        if (value === undefined) {
            if (this.get('isOneSelected')) {
                return this.filterBy('selected')[0];
            } else {
                return null;
            }
        } else {
            return value;
        }
    }.property('@each.selected'),

    currentFileName: function (key, value) {
        if (value === undefined) {
            var currentFile = this.get('currentFile');
            if (currentFile) {
                return currentFile.get('name');
            } else {
                return '';
            }
        } else {
            return value;
        }
    }.property('currentFile'),

    currentFileAttr: function (key, value) {
        if (value === undefined) {
            var currentFile = this.get('currentFile');
            if (currentFile) {
                return currentFile.get('attribute');
            } else {
                return '';
            }
        } else {
            return value;
        }
    }.property('currentFile'),

    actions: {
        createNewFile: function () {
            var name = this.get('newFileName');
            var attr = this.get('newFileAttr');
            this.set('newFileName', '');
            this.set('newFileAttr', '');
            if (name && attr) {
                var file = this.store.createRecord('file', {
                    name: name,
                    attribute: attr,
                    selected: false
                });

                file.save();
            }
        },

        renameFile: function () {
            var name = this.get('currentFileName');
            if (name) {
                var file = this.get('currentFile');
                file.set('name', name);
                file.save();
            }
        },

        changeAttr: function () {
            var attr = this.get('currentFileAttr');
            if (attr) {
                var selectedFiles = this.filterBy('selected');
                selectedFiles.forEach(function (file) {
                    file.set('attribute', attr);
                    file.save();
                });
            }
        },

        selectRow: function (file) {
            file.set('selected', !file.get('selected'));
        },

        remove: function () {
            //var file = this.get('model');
            //console.log('remove ' + file);
            var selected = this.filterBy('selected', true);
            selected.invoke('deleteRecord');
            selected.invoke('save');
        }
    }
});