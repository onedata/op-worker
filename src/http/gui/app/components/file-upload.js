import Ember from 'ember';

export default Ember.Component.extend({
  fileUploadService: Ember.inject.service('file-upload'),
  notify: Ember.inject.service(),
  oneproviderServer: Ember.inject.service(),

  /** Dir to put files into */
  dir: null,

  uploadAddress: '/upload',

  resumable: function() {
    return this.get('fileUploadService.resumable');
  }.property('fileUploadService.resumable'),

  onFileAdded: function() {
    return (file) => {
      this.$().show();
      // Show progress bar
      $('.resumable-progress, .resumable-list').show();
      // Show pause, hide resume
      $('.resumable-progress .progress-resume-link').hide();
      $('.resumable-progress .progress-pause-link').show();
      // Add the file to the list
      $('.resumable-list').append('<li class="resumable-file-'+file.uniqueIdentifier+'">Uploading <span class="resumable-file-name"></span> <span class="resumable-file-progress"></span>');
      $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-name').html(file.fileName);

      this.get('notify').info('Starting file upload: ' + file.fileName);
      this.get('resumable').upload();
    };
  }.property(),

  onPause: function() {
    return function() {
      // Show resume, hide pause
      $('.resumable-progress .progress-resume-link').show();
      $('.resumable-progress .progress-pause-link').hide();
    };
  }.property(),

  onComplete: function() {
    return () => {
      // Hide pause/resume when the upload has completed
      $('.resumable-progress .progress-resume-link, .resumable-progress .progress-pause-link').hide();
      this.$().hide();
    };
  }.property(),

  onFileSuccess: function() {
    return (file/*, message*/) => {
      $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html('(completed)');
      this.get('notify').info(`File "${file.fileName}" uploaded successfully!`);
      this.get('oneproviderServer').fileUploadComplete(file.uniqueIdentifier);
    };
  }.property(),

  onFileError: function() {
    return (file, message) => {
      $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html('(file could not be uploaded: '+message+')');
      this.get('notify').error(`File "${file.fileName}" upload failed: ${message}`);
      this.get('oneproviderServer').fileUploadComplete(file.uniqueIdentifier);
    };
  }.property(),

  onFileProgress: function() {
    let r = this.get('resumable');
    return function(file) {
      // Handle progress for both the file and the overall upload
      $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html(Math.floor(file.progress()*100) + '%');
      $('.progress-bar').css({width:Math.floor(r.progress()*100) + '%'});
    };
  }.property(),

  onCancel: function() {
    return function() {
      $('.resumable-file-progress').html('canceled');
    };
  }.property(),

  onUploadStart: function() {
    return () => {
      // Show pause, hide resume
      $('.resumable-progress .progress-resume-link').hide();
      $('.resumable-progress .progress-pause-link').show();
    };
  }.property(),

  didInsertElement() {
    this.$().hide();

    let r = this.get('fileUploadService.resumable');

    // TODO: use component selector this.$().find(...)

    if (!r.support) {
      // TODO: transalte or other message
      this.get('notify').warning('ResumableJS is not supported in this browser!');
      $('.resumable-error').show();
    }

    r.on('fileAdded', this.get('onFileAdded'));
    r.on('pause', this.get('onPause'));
    r.on('complete', this.get('onComplete'));
    r.on('fileSuccess', this.get('onFileSuccess'));
    r.on('fileError', this.get('onFileError'));
    r.on('fileProgress', this.get('onFileProgress'));
    r.on('cancel', this.get('onCancel'));
    r.on('uploadStart', this.get('onUploadStart'));
  },

  willDestroyElement() {
    // a hack to reset all events bound previously
    // NOTE that it will destroy all events, even bound in another code!
    this.get('resumable').events = [];
  },

  registerComponentInService: function() {
    this.set('fileUploadService.component', this);
  }.on('init'),
});
