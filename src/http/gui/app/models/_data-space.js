import DS from 'ember-data';

/**
 * A space for files. It has a reference to root dir with it's files.
 * @module models/_data-space
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default DS.Model.extend({
  /** Name exposed in GUI */
  name: DS.attr('string'),

  /** A root directory with space files. It must be a "dir" file! */
  rootDir: DS.belongsTo('file'),

  validateRootDir: function() {
    let rootDir = this.get('rootDir');
    if (!rootDir.get('isDir')) {
      console.error(`DataSpace ${this.get('id')} rootDir has been set to non dir file!`);
    }
  }.observes('rootDir')
});
