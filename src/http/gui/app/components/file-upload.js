import Ember from 'ember';
/* globals Resumable */

// TODO: this component will become a bottom progress panel
export default Ember.Component.extend({
  fileUploadService: Ember.inject.service('file-upload'),
  notify: Ember.inject.service('notify'),

  /** Dir to put files into */
  dir: null,

  uploadAddress: '/upload',

  resumable: function() {
    if (!this.get('dir.id')) {
      return null;
    }

    console.debug(`Crenating new Resumable with dir id: ${this.get('dir.id')}`);
    let r = new Resumable({
      target: this.get('uploadAddress'),
      chunkSize: 1*1024*1024,
      simultaneousUploads: 4,
      testChunks: false,
      throttleProgressCallbacks: 1,
      query: {parentId: this.get('dir.id')}
    });

    Ember.run.scheduleOnce('afterRender', this, function() {
      $('.file-upload').hide();

      if (!r.support) {
        // TODO: other message
        this.get('notify').warning('ResumableJS is not supported in this browser!');
        $('.resumable-error').show();
      }

      r.on('fileAdded', (file) => {
        $('.file-upload').show();
        // Show progress bar
        $('.resumable-progress, .resumable-list').show();
        // Show pause, hide resume
        $('.resumable-progress .progress-resume-link').hide();
        $('.resumable-progress .progress-pause-link').show();
        // Add the file to the list
        $('.resumable-list').append('<li class="resumable-file-'+file.uniqueIdentifier+'">Uploading <span class="resumable-file-name"></span> <span class="resumable-file-progress"></span>');
        $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-name').html(file.fileName);

        this.get('notify').info('Starting file upload: ' + file.fileName);
        r.upload();
      });

      r.on('pause', function(){
        // Show resume, hide pause
        $('.resumable-progress .progress-resume-link').show();
        $('.resumable-progress .progress-pause-link').hide();
      });

      r.on('complete', () => {
        // Hide pause/resume when the upload has completed
        $('.resumable-progress .progress-resume-link, .resumable-progress .progress-pause-link').hide();
        $('.file-upload').hide();
      });

      r.on('fileSuccess', (file/*, message*/) => {
        $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html('(completed)');
        this.get('notify').info(`File "${file.fileName}" uploaded successfully!`);
      });

      r.on('fileError', (file, message) => {
        $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html('(file could not be uploaded: '+message+')');
        this.get('notify').error(`File "${file.fileName}" upload failed: ${message}`);
      });

      r.on('fileProgress', function(file){
        // Handle progress for both the file and the overall upload
        $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html(Math.floor(file.progress()*100) + '%');
        $('.progress-bar').css({width:Math.floor(r.progress()*100) + '%'});
      });

      r.on('cancel', function(){
        $('.resumable-file-progress').html('canceled');
      });

      r.on('uploadStart', () => {
        // Show pause, hide resume
        $('.resumable-progress .progress-resume-link').hide();
        $('.resumable-progress .progress-pause-link').show();
      });
    });

    return r;
  }.property('uploadAddress', 'dir', 'dir.id'),

  registerComponentInService: function() {
    this.get('fileUploadService').set('fileUploadComponent', this);
  }.on('init'),

  /// Service API
  assignBrowse(jqBrowseElement) {
    this.get('resumable').assignBrowse(jqBrowseElement[0]);
  },

  assignDrop(jqDropElement) {
    this.get('resumable').assignDrop(jqDropElement[0]);

    let lastEnter;

    let startDrag = function(event) {
      lastEnter = event.target;
      jqDropElement.addClass('file-drag');
    };

    let endDrag = function(event) {
      if (lastEnter === event.target) {
        jqDropElement.removeClass('file-drag');
      }
    };

    jqDropElement.on('dragenter', startDrag);
    jqDropElement.on('dragleave', endDrag);
    jqDropElement.on('dragend', endDrag);
    jqDropElement.on('drop',  endDrag);
  },



  // TODO: does not react to upladAddress changes
  // TODO: use element selectors within this widget
  // old() {
  //   var r = this.get('fileUpload.resumable');
  //   // Resumable.js isn't supported, fall back on a different method
  //   if (!r.support) {
  //     // TODO: other message
  //     window.alert('resumable js is not supported!');
  //     $('.resumable-error').show();
  //   } else {
  //     // Show a place for dropping/selecting files
  //     $('.resumable-drop').show();
  //     r.assignDrop($('.resumable-drop')[0]);
  //     r.assignBrowse($('.resumable-browse')[0]);
  //
  //     // Handle file add event
  //     r.on('fileAdded', function(file){
  //         // Show progress pabr
  //         $('.resumable-progress, .resumable-list').show();
  //         // Show pause, hide resume
  //         $('.resumable-progress .progress-resume-link').hide();
  //         $('.resumable-progress .progress-pause-link').show();
  //         // Add the file to the list
  //         $('.resumable-list').append('<li class="resumable-file-'+file.uniqueIdentifier+'">Uploading <span class="resumable-file-name"></span> <span class="resumable-file-progress"></span>');
  //         $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-name').html(file.fileName);
  //         // Actually start the upload
  //         r.upload();
  //       });
  //     r.on('pause', function(){
  //         // Show resume, hide pause
  //         $('.resumable-progress .progress-resume-link').show();
  //         $('.resumable-progress .progress-pause-link').hide();
  //       });
  //     r.on('complete', function(){
  //         // Hide pause/resume when the upload has completed
  //         $('.resumable-progress .progress-resume-link, .resumable-progress .progress-pause-link').hide();
  //       });
  //     r.on('fileSuccess', function(file,message){
  //         // Reflect that the file upload has completed
  //         $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html('(completed)');
  //       });
  //     r.on('fileError', function(file, message){
  //         // Reflect that the file upload has resulted in error
  //         $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html('(file could not be uploaded: '+message+')');
  //       });
  //     r.on('fileProgress', function(file){
  //         // Handle progress for both the file and the overall upload
  //         $('.resumable-file-'+file.uniqueIdentifier+' .resumable-file-progress').html(Math.floor(file.progress()*100) + '%');
  //         $('.progress-bar').css({width:Math.floor(r.progress()*100) + '%'});
  //       });
  //     r.on('cancel', function(){
  //       $('.resumable-file-progress').html('canceled');
  //     });
  //     r.on('uploadStart', function(){
  //         // Show pause, hide resume
  //         $('.resumable-progress .progress-resume-link').hide();
  //         $('.resumable-progress .progress-pause-link').show();
  //     });
  //   }
  // }
});
