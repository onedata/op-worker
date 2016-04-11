import Ember from 'ember';
/* globals Resumable */

export default Ember.Service.extend({
  component: null,

  /** Current dir for upload - global for application! */
  dir: null,

  resumable: function() {
    console.debug(`Creating new Resumable`);
    return new Resumable({
      target: '/upload',
      chunkSize: 1*1024*1024,
      simultaneousUploads: 4,
      testChunks: false,
      throttleProgressCallbacks: 1,
      query: () => {
        console.debug(`Will upload to dir: ${this.get('dir.name')}, id: ${this.get('dir.id')}`);
        return {
          parentId: this.get('dir.id')
        };
      },
      generateUniqueIdentifier: function() {
        let date = new Date().getTime();
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g,
          function (character) {
            let random = (date + Math.random() * 16) % 16 | 0;
            date = Math.floor(date / 16);
            return (character === 'x' ? random : (random & 0x7 | 0x8)).toString(16);
          });
      }
    });
  }.property(),

  assignDrop(jqDropElement) {
    this.get('resumable').assignDrop(jqDropElement);

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

  assignBrowse(jqBrowseElement) {
    this.get('resumable').assignBrowse(jqBrowseElement);
  }
});
