import Ember from 'ember';

/**
 * Load model for space - to be able to browse it's root dir.
 *
 * @module routes/data
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default Ember.Route.extend({
  // TODO: enable this
  // model(params) {
  //   return this.store.peekRecord('dataSpace', params.data_space_id);
  // },

  model(params) {
    // TODO: use dataSpaceId to fetch root files for DataSpace
    let dataSpaceId = params.data_space_id;
    // TODO: real store
    return {
      id: dataSpaceId,
      name: `data-space-${dataSpaceId}`,
      rootDir: {
        id: 'lol-root-dir'
      }
    };
  }
});
