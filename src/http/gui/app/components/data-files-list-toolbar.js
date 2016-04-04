import Ember from 'ember';

export default Ember.Component.extend({
  notify: Ember.inject.service('notify'),
  fileUpload: Ember.inject.service('fileUpload'),
  store: Ember.inject.service(),

  tagName: 'ul',
  classNames: ['nav', 'navbar-nav', 'navbar-right', 'toolbar-group'],

  fileForChunks: null,

  /**
   * Holds items of toolbar. Each item is a Object with properties:
   * - icon {String}
   * - action {String} - name of action to invoke when clicked,
   *     the function should be parameterless
   * - disabled {Boolean}
   */
  items: function() {
    let i18n = this.get('i18n');
    return [
      {
        id: 'create-dir-tool',
        icon: 'folder-new',
        action: 'createDir',
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.createDir')
      },
      // TODO: temporary, to decide
      {
        id: 'create-file-tool',
        icon: 'file',
        action: 'createFile',
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.createFile')
      },
      {
        id: 'share-file-tool',
        icon: 'share',
        action: 'notImplemented',
        //disabled: !this.get('dir.isSomeFileSelected'),
        disabled: true,
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.shareFile')
      },
      // using fileUpload service binding
      {
        id: 'upload-file-tool',
        icon: 'upload',
        action: 'uploadBrowse',
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.uploadFile')
      },
      {
        id: 'rename-file-tool',
        icon: 'rename',
        action: 'renameSelectedFile',
        //disabled: !this.get('dir.singleSelectedFile'),
        disabled: true,
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.renameFile')
      },
      {
        id: 'lock-file-tool',
        icon: 'lock',
        action: 'editPermissions',
        disabled: !this.get('dir.isSomeFileSelected'),
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.permissions')
      },
      {
        id: 'copy-file-tool',
        icon: 'copy',
        action: 'notImplemented',
        //disabled: !this.get('dir.isSomeFileSelected'),
        disabled: true,
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.copy')
      },
      {
        id: 'cut-file-tool',
        icon: 'cut',
        action: 'notImplemented',
        //disabled: !this.get('dir.isSomeFileSelected'),
        disabled: true,
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.cut')
      },
      {
        id: 'remove-file-tool',
        icon: 'remove',
        action: 'removeSelectedFiles',
        disabled: !this.get('dir.isSomeFileSelected'),
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.remove')
      },
      {
        id: 'file-chunks-tool',
        icon: 'provider',
        action: 'showChunks',
        disabled: !this.get('dir.singleSelectedFile'),
        tooltip: i18n.t('components.dataFilesListToolbar.tooltip.chunks')
      },
    ];
  }.property('dir.isSomeFileSelected', 'dir.singleSelectedFile'),

  makeTooltips: function() {
    Ember.run.scheduleOnce('afterRender', this, function() {
      this.$().find('[data-toggle="tooltip"]').tooltip();
    });
  }.observes('items', 'fileBlocks.@each.provider.@each.name'),

  didInsertElement() {
    this.makeTooltips();
    console.debug('Binding upload button for files toolbar');
    // NOTE: file upload component has dir set by data-files-list component,
    // so data-files-list _must_ be used when using this toolbar
    // if this changes - please copy "dirChanged" method from files-list here
    this.get('fileUpload').assignBrowse(this.$().find('#toolbar-file-browse'));
  },

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

    showChunks() {
      this.set('isFileChunksModal', true);
      this.set('fileForChunks', this.get('dir.singleSelectedFile'));
      let fileId = this.get('fileForChunks.id');
      // TODO: if fileId null...

      this.get('store').query('file-distribution', { filter: { fileId: fileId } }).then(
        (fbs) => {
          this.set('fileBlocks', fbs);
        },
        (error) => {
          console.error('Error loading file blocks: ' + error);
        }
      );
    },

    // TODO: set fileForChunks to null on close

    uploadBrowse() {
      this.$('#toolbar-file-browse').trigger('click');
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
