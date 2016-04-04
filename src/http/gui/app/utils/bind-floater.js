// TODO: move this util to deps/gui

/**
  Makes an element fixed positioned, with coordinates (left, top) of its parent.

  @param element A jQuery element which will float (only single!)
  @param parent (optional) A jQuery element which will act as element position parent.
          If not provided - will use a element.parent().
  @returns {function} Function which re-computes new fixed position of an element.
              It should be bind to eg. parent mouseover.
 */
export default function bindFloater(element, parent, options) {
  parent = parent || element.parent();
  // default options
  options = options || {};
  options.posX = options.posX || 'right';
  options.posY = options.posY || 'top';
  options.offsetX = options.offsetX || 0;
  options.offsetY = options.offsetY || 0;

  element.addClass('floater');
  let changePos = function() {
    let offset = parent.offset();
    let left;
    if (options.posX === 'right') {
      left = `${parseInt(offset.left) + parent.width()}px`;
    } else if (options.posX === 'left') {
      left = `${parseInt(offset.left) - element.width()}px`;
    }
    let top;
    if (options.posY === 'top') {
      top = offset.top;
    } else if (options.posY === 'middle') {
      top = `${parseInt(offset.top) - element.height()/2}px`;
    }

    element.css({
      left: `${parseInt(left) + options.offsetX - $(window).scrollLeft()}px`,
      top: `${parseInt(top) + options.offsetY + $(window).scrollTop()}px`
    });
    // element.css({
    //   left: left,
    //   top: top
    // });
  };

  changePos();
  return changePos;
}
