// This is a prototype controller for file list view.
// It creates an interface between the view and model.

import Ember from 'ember';

export default Ember.Component.extend({
  // Sorting of files by type and name
  sortProperties: ['type:asc', 'name:asc'],
  sortedModel: Ember.computed.sort("model", "sortProperties"),

  // The space that is currently browsed
  currentSpaceId: null,
  currentSpace: null,

  // Creating new files / dirs
  newFileName: '',
  newFileParent: 'root',
  fetchNewFileParentId: function () {
    if (this.get('isOneSelected')) {
      var selected = this.get('model').findBy('selected', true);
      if (selected.get('type') == 'dir') {
        // Set new ID only if one directory is selected
        this.set('newFileParent', selected);
      }
    } else {
      this.set('newFileParent', this.get('currentSpace'));
    }
    console.log('newFileParent ' + this.get('newFileParent.id'));
  }.observes('currentSpace,@each.selected'),

  // File preview
  previewedFile: null,
  previewedFileContent: null,
  fetchPreviewedFileContent: function () {
    if (this.get('previewedFile')) {
      var fileContentId = 'content#' + this.get('previewedFile.id');
      console.log('fileContentId: ' + fileContentId);
      var controller = this;
      this.store.find('fileContent', fileContentId).then(function (data) {
        controller.set('previewedFileContent', data);
      });
    }
  }.observes('previewedFile'),
  editingPreview: false,
  editAreaDisabled: Ember.computed.not('editingPreview'),

  // Resolving current space
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
    }
  }.observes('currentSpaceId'),

  // A virtual dir that contains all spaces
  spacesDir: function () {
    return this.get('model').findBy('id', 'root')
  }.property(),

  // Number of selected files
  selectedCount: function () {
    var visibleFiles = this.get('model').filterBy('isVisible');
    var res = visibleFiles.filterBy('selected').length;
    console.log('selectedCount: ' + res);
    return res;
  }.property('@each.selected'),

  areAllSelected: function (key, value) {
    if (value === undefined) {
      return !!this.get('model').get('length') &&
        this.get('model').filterBy('isVisible').isEvery('selected', true);
    } else {
      this.get('model').filterBy('isVisible').setEach('selected', value);
      return value;
    }
  }.property('@each.isVisible,@each.selected'),

  isAnySelected: function () {
    return this.get('selectedCount') > 0;
  }.property('selectedCount'),
  isNoneSelected: Ember.computed.not('isAnySelected'),

  isOneSelected: function () {
    return this.get('selectedCount') == 1;
  }.property('selectedCount'),
  isNotOneSelected: Ember.computed.not('isOneSelected'),

  // File that is selected, if only it is selected
  currentFile: function (key, value) {
    console.log('key ' + key);
    console.log('value ' + value);
    if (value === undefined) {
      if (this.get('isOneSelected')) {
        return this.get('model').findBy('selected');
      } else {
        return null;
      }
    } else {
      return value;
    }
  }.property('isOneSelected'),

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

  // Handling actions
  actions: {
    createNewDir: function () {
      this.send('createNew', 'dir');
    },

    createNewFile: function () {
      this.send('createNew', 'file');
    },

    createNew: function (type) {
      var name = this.get('newFileName');
      this.set('newFileName', '');
      var parentID = this.get('newFileParent.id');
      if (name) {
        var file = this.store.createRecord('file', {
          name: name,
          type: type,
          parentId: parentID
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

    fileClicked: function (file) {
      if (this.get('previewedFile')) {
        this.get('previewedFile').set('expanded', false);
      }
      this.set('previewedFile', null);
      this.set('editingPreview', false);
      if (file.get('type') == 'dir') {
        file.set('expanded', !file.get('expanded'));
      } else {
        file.set('expanded', true);
        this.set('previewedFile', file);
      }
    },

    remove: function () {
      var selected = this.get('model').filterBy('selected', true);
      selected.invoke('deleteRecord');
      selected.invoke('save');
    },

    selectAll: function () {
      var visibleFiles = this.get('model').filterBy('isVisible');
      visibleFiles.setEach('selected', true);
    },

    deselectAll: function () {
      var visibleFiles = this.get('model').filterBy('isVisible');
      visibleFiles.setEach('selected', false);
    },

    editPreview: function () {
      this.set('editingPreview', true);
    },

    savePreview: function () {
      this.get('previewedFileContent').save();
      this.set('editingPreview', false);
    },

    discardPreview: function () {
      this.get('previewedFileContent').rollback();
      this.set('editingPreview', false);
    },

    showModal: function (modal_type) {
      this.sendAction('modalAction', 'showModal', modal_type, this);
    }
  }
});
