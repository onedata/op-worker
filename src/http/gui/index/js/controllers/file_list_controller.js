App.FileListController = Ember.ArrayController.extend({
    sortProperties: ['type:asc', 'name:asc'],
    sortedModel: Ember.computed.sort("model", "sortProperties"),
    currentSpaceId: null,
    currentSpace: null,
    previewedFile: null,

    fetchCurrentSpace: function () {
        if (this.get('currentSpaceId')) {
            console.log('currentSpaceId ' + this.get('currentSpaceId'));
            var spaceId = this.get('currentSpaceId');
            spaceId = spaceId.substring(spaceId.indexOf('#') + 1);
            console.log('currentSpaceId ' + spaceId);
            console.log('currentSpace ' + this.findBy('id', spaceId).get('name'));
            var controller = this;
            this.store.find('file', spaceId).then(function (data) {
                data.set('expanded', true);
                controller.set('currentSpace', data);
                console.log(controller.get('currentSpace'));
            });
            //return this.findBy('id', spaceId);
            //console.log('spaceId ' + spaceId);
            //var file = this.findBy('id', spaceId);
            //console.log('file ' + file);
            //var current = this.findBy('id', this.get('currentSpaceId'));
            //current.set('expanded', true);
            //return this.findBy('id', this.get('currentSpaceId'));
        }
        //else {
        //return null;
        //}
    }.observes('currentSpaceId'),

    visibleDirs: function () {
        return this.filter(function (item, index, enumerable) {
            console.log('item: ' + item.get('id') + ' ' + item.get('expanded'));
            return item.get('expanded') || item.get('parent.expanded');
        });
    }.property('@each.expanded'),

    spacesDir: function () {
        return this.findBy('id', 'root')
    }.property('@each.selected'),

    selectedCount: function () {
        return this.filterBy('selected').length;
    }.property('@each.selected'),

    areAllSelected: function (key, value) {
        var visibleFiles = this.filterBy('isVisible');
        if (value === undefined) {
            return !!visibleFiles.get('length') && visibleFiles.isEvery('selected', true);
        } else {
            visibleFiles.setEach('selected', value);
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

        fileClicked: function (file) {
            if (this.get('previewedFile')) {
                this.get('previewedFile').set('expanded', false);
            }
            this.set('previewedFile', null);
            if (file.get('type') == 'dir') {
                file.set('expanded', !file.get('expanded'));
            } else {
                file.set('expanded', true);
                this.set('previewedFile', file)
            }
        },

        remove: function () {
            //var file = this.get('model');
            //console.log('remove ' + file);
            var selected = this.filterBy('selected', true);
            selected.invoke('deleteRecord');
            selected.invoke('save');
        },

        selectAll: function () {
            var visibleFiles = this.filterBy('isVisible');
            visibleFiles.setEach('selected', true);
        },

        deselectAll: function () {
            var visibleFiles = this.filterBy('isVisible');
            visibleFiles.setEach('selected', false);
        }
    }
});