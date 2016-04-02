import Ember from 'ember';
import FileChunksBar from '../utils/file-chunks-bar';

export default Ember.Component.extend({
  file: null,
  fileBlocks: null,

  didInsertElement() {
    this.set('canvas', this.$().find('canvas'));
    this.redrawCanvas();
  },

  // should use new everytime?
  redrawCanvas: function() {

    if (this.get('file.size') && this.get('fileBlocks.blocks')) {
      new FileChunksBar(this.get('canvas'), {
        file_size: this.get('file.size'),
        chunks: this.get('fileBlocks.blocks')
      });
    }
  }.observes('canvas', 'file', 'file.size', 'fileBlocks', 'fileBlocks.blocks')
});
