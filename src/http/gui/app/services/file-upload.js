import Ember from 'ember';
/* globals Resumable */

export default Ember.Service.extend({
  uploadAddress: '/upload',
  resumable: function() {
    return new Resumable({
      target: this.get('uploadAddress'),
      chunkSize: 1*1024*1024,
      simultaneousUploads: 4,
      testChunks: false,
      throttleProgressCallbacks: 1,
      query: {parentId: this.get('currentDir.id')}
    });
  }.property('uploadAddress'),
});
