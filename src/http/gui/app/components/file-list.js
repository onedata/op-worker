// This is a prototype controller for file list view.
// It creates an interface between the view and model.

import Ember from 'ember';

export default Ember.Component.extend({
  store: null,

  // Sorting of files by type and name
  sortProperties: ['type:asc', 'name:asc'],
  sortedChildren: Ember.computed.sort('currentSpace.children', 'sortProperties'),

  // The space that is currently browsed
  currentSpaceId: null,
  currentSpace: null,

  // Inputting new dir/file name
  creatingFile: false,
  createdFileType: 'file',
  createdFileHint: 'New file name',
  createdFileName: '',

  // Creating new files / dirs
  newFileName: '',
  newFileParent: null,
  fetchNewFileParentId: function () {
    if (this.get('isOneSelected')) {
      var selected = this.get('model').findBy('selected', true);
      if (selected.get('type') === 'dir') {
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
      this.get('store').find('fileContent', fileContentId).then(function (data) {
        controller.set('previewedFileContent', data);
      });
    }
  }.observes('previewedFile'),
  editingPreview: false,
  editAreaDisabled: Ember.computed.not('editingPreview'),

  // Resolving current space
  fetchCurrentSpace: function () {
    console.log('fetchCurrentSpace ' + this.get('currentSpaceId'));
    console.log('spinner ' + this.$('#select-space').val());
    if (this.get('currentSpaceId')) {
      console.log('currentSpaceId ' + this.get('currentSpaceId'));
      var spaceId = this.get('currentSpaceId');
      spaceId = spaceId.substring(spaceId.indexOf('#') + 1);
      console.log('currentSpaceId ' + spaceId);
      var controller = this;
      this.get('store').find('file', spaceId).then(function (data) {
        data.set('expanded', true);
        controller.set('currentSpace', data);
        console.log(controller.get('currentSpace'));
      });
    }
  }.observes('currentSpaceId'),

  // A virtual dir that contains all spaces
  spacesDir: function () {
    return this.get('model').findBy('id', 'root');
  }.property(),
  spaceDirOptions: Ember.computed('spacesDir.children,currentSpaceId', function () {
    var that = this;
    return this.get('spacesDir.children').map(function (space) {
      return {
        space: space,
        selected: that.get('currentSpaceId') === space.get('id')
      };
    });
  }),

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
    return this.get('selectedCount') === 1;
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
    changeCurrentDir: function (newSpaceId) {
      this.set('currentSpaceId', newSpaceId);
    },

    editNewDir: function () {
      this.set('creatingFile', true);
      this.set('createdFileType', 'dir');
      this.set('createdFileHint', 'New directory name');
    },

    editNewFile: function () {
      this.set('creatingFile', true);
      this.set('createdFileType', 'file');
      this.set('createdFileHint', 'New file name');
    },

    createNewFile: function () {
      var type = this.get('createdFileType');
      var name = this.get('createdFileName');
      this.set('createdFileName', '');
      var parentID = this.get('newFileParent.id');
      this.set('creatingFile', false);
      if (name) {
        this.sendAction('createNewFileAction', name, type, parentID);
        //var file = this.get('store').createRecord('file', {
        //  name: name,
        //  type: type,
        //  parentId: parentID
        //});
        //file.save();
      }
    },

    fileClicked: function (file) {
      if (this.get('previewedFile')) {
        this.get('previewedFile').set('expanded', false);
      }
      this.set('previewedFile', null);
      this.set('editingPreview', false);
      if (file.get('type') === 'dir') {
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
    }
  }
});
