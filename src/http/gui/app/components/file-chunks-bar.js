import Ember from 'ember';
import FileChunksBar from '../utils/file-chunks-bar';

export default Ember.Component.extend({
  file: null,
  fileBlocks: null,

  didInsertElement() {
    this.set('canvas', this.$().find('canvas'));
    this.redrawCanvas();
  },

  isRendered: false,
  isRenderFailed: false,
  isLoading: false,

  // should use new everytime?
  redrawCanvas: function() {

    if (this.get('file.size') && this.get('fileBlocks.blocks')) {
      try {
        this.set('isRenderFailed', false);
        this.set('isLoading', true);
        new FileChunksBar(this.get('canvas'), {
          file_size: this.get('file.size'),
          chunks: this.get('fileBlocks.blocks')
        });
        this.set('isLoading', false);
        this.set('isRendered', true);
      } catch (error) {
        this.set('isRenderFailed', false);
      }
    }
  }.observes('canvas', 'file', 'file.size', 'fileBlocks', 'fileBlocks.blocks')
});
