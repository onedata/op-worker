import Ember from 'ember';

export default Ember.Component.extend({
  notify: Ember.inject.service('notify'),

  tagName: 'ul',
  classNames: ['nav', 'navbar-nav', 'navbar-right', 'toolbar-group'],

  /**
   * Holds items of toolbar. Each item is a Object with properties:
   * - icon {String}
   * - action {String} - name of action to invoke when clicked,
   *     the function should be parameterless
   * - disabled {Boolean}
   */
  items: function() {
    return [
      {
        icon: 'folder-new',
        action: 'createDir',
      },
      // TODO: temporary, to decide
      {
        icon: 'file',
        action: 'createFile',
      },
      {
        icon: 'share',
        action: 'notImplemented',
        disabled: !this.get('dir.isSomeFileSelected'),
      },
      // TODO: invoke uploader on action
      {
        icon: 'upload',
        action: 'notImplemented',
      },
      {
        icon: 'rename',
        action: 'renameSelectedFile',
        disabled: !this.get('dir.singleSelectedFile'),
      },
      {
        icon: 'lock',
        action: 'editPermissions',
        disabled: !this.get('dir.isSomeFileSelected'),
      },
      {
        icon: 'copy',
        action: 'notImplemented',
        disabled: !this.get('dir.isSomeFileSelected'),
      },
      {
        icon: 'cut',
        action: 'notImplemented',
        disabled: !this.get('dir.isSomeFileSelected'),
      },
      {
        icon: 'remove',
        action: 'removeSelectedFiles',
        disabled: !this.get('dir.isSomeFileSelected'),
      },
    ];
  }.property('dir.isSomeFileSelected', 'dir.singleSelectedFile'),

  actions: {
    /// Actions on toolbar items click

    renameSelectedFile() {
      if (this.get('dir.singleSelectedFile')) {
        this.set('renameFileName', '');
        this.set('isRenamingFile', true);
      }
    },

    createDir() {
      this.set('createFileName', '');
      this.set('isCreatingDir', true);
    },

    createFile() {
      this.set('createFileName', '');
      this.set('isCreatingFile', true);
    },

    renameModalOpened() {
      // TODO: should use autofocus of modal bs-form-element, but it does not work
      // $('*').focus(function(event) {
      //   debugger;
      // });

      this.$().find('input').focus().select();
    },

    // TODO: error handling
    removeSelectedFiles() {
      this.set('isRemovingFiles', true);
    },

    editPermissions() {
      this.set('newPermissions', '');
      this.set('isEditingPermissions', true);
    },

    notImplemented() {
      this.set('isNotImplementedModal', true);
    },

    /// Actions for modals
    // TODO: move modals to separate components? (they have some state)

    submitRenameSelectedFile() {
      try {
        let file = this.get('dir.singleSelectedFile');
        if (file) {
          if (this.get('renameFileName')) {
            file.set('name', this.get('renameFileName') || '');
            file.save();
          } else {
            console.error('Please enter non-blank file name');
          }
        } else {
          console.error('No file selected to rename or multiple selected');
        }
      } finally {
        this.set('isRenamingFile', false);
      }
    },

    submitCreateFile(type) {
      try {
        this.get('dir').createFile(type, this.get('createFileName'));
      } finally {
        this.set('isCreatingFile', false);
        this.set('isCreatingDir', false);
      }
    },

    submitRemoveFiles() {
      try {
        this.get('dir').removeSelectedFiles();
        this.get('notify').success('Files removed');
      } finally {
        this.set('isRemovingFiles', false);
      }
    },

    submitEditPermissions() {
      try {
        this.get('dir').setSelectedFilesPermissions(this.get('newPermissions'));
      } finally {
        this.set('isEditingPermissions', false);
      }
    },
  }
});
