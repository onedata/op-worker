import Ember from 'ember';

export default Ember.Component.extend({
  tagName: 'ul',
  classNames: ['nav', 'navbar-nav', 'navbar-right', 'toolbar-group'],

  someInput: null,

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
        action: 'notImplemented',
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

    // TODO: show modal with input text
    createDir() {
      this.get('dir').createFile('dir', this.get('someInput'));
    },

    createFile() {
      this.get('dir').createFile('file', this.get('someInput'));
    },

    renameSelectedFile() {
      if (this.get('dir.singleSelectedFile')) {
        this.set('renameFileName', '');
        this.set('isRenamingFile', true);
      }
    },

    renameModalOpened() {
      // TODO: should use autofocus of modal bs-form-element, but it does not work
      // $('*').focus(function(event) {
      //   debugger;
      // });

      this.$().find('input').focus();
    },

    // TODO: error handling
    removeSelectedFiles() {
      this.get('dir').removeSelectedFiles();
    },

    notImplemented() {
      window.alert('not implemented yet!');
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
  }
});
