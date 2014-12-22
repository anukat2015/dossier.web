/**
 * @file Sorting Queue component.
 * @copyright 2014 Diffeo
 *
 * @author Miguel Guedes <miguel@miguelguedes.org>
 *
 * Comments:
 *
 *
 */


/*global jQuery, define */
/*jshint laxbreak:true */


/**
 * The Sorting Queue module.
 *
 * @returns an object containing class constructors.
 * */
var SortingQueue_ = function (window, $) {

  /**
   * @class
   * */
  /**
   * Constructor responsible for initialising Sorting Queue.
   *
   * @param   {Object}    opts  Initialisation options (please refer to
   *                            `defaults_' above)
   * @param   {Object}    cbs   Map of all callbacks
   *
   * @param   cbs.moreTexts           Retrieve additional text items.
   * @param   cbs.itemDismissed       Event triggered when a text item is
   *                                  dismissed.
   * @param   cbs.itemSelected        Event triggered when a text item is
   *                                  selected.
   * @param   cbs.itemDeselected      Event triggered when a text item is
   *                                  deselected.
   * @param   cbs.onRequestStart      Executed after request initiated.
   * @param   cbs.onRequestStop       Executed after request finished.
   * */
  var Sorter = function (opts, cbs)
  {
    this.resetter_ = false;

    /* Allow a jQuery element to be passed in instead of an object containing
     * options. In the case that a jQuery element is detected, it is assumed to
     * be the `nodes.items' element. */
    if(!opts)
      throw "No options given: some are mandatory";
    else if(opts instanceof $) {
      opts = {
        nodes: {
          items: opts
        }
      };
    } else if(!opts.nodes)
      throw "No nodes options given: `items' required";
    else if(!opts.nodes.items)
      throw "Missing `items' nodes option";

    /* Allow a function to be passed in instead of an object containing
     * callbacks. In the case that a function is passed in, it is assumed to be
     * the `moreTexts' callback. */
    if(!cbs)
      throw "No callbacks given: some are mandatory";
    else if(cbs instanceof Function) {
      cbs = {
        moreTexts: cbs
      };
    } else if(!cbs.moreTexts)
      throw "Mandatory `moreTexts' callback missing";

    console.log("Initialising Sorting Queue UI");

    this.options_ = $.extend(true, $.extend(true, {}, defaults_), opts);

    /* Begin instantiating and initialising controllers. */
    (this.callbacks_ = new ControllerCallbacks(
      this,
      $.extend(true, {
        itemDismissed: function() {},
        itemSelected: function() {},
        itemDeselected: function() {},
        onRequestStart: function() {},
        onRequestStop: function() {}
      }, cbs)))
        .initialise();

    (this.requests_ = new ControllerRequests(this))
      .initialise();
  };

  Sorter.prototype = {
    initialised_: false,
    resetter_: false,
    options_: null,
    /* Controllers */
    callbacks_: null,
    requests_: null,
    dismiss_: null,
    keyboard_: null,
    items_: null,

    initialise: function ()
    {
      var self = this;

      if(this.initialised_)
        throw "Already initialised";

      if(!this.options_.nodes.buttonDismiss)
        this.options_.nodes.buttonDismiss = $();

      (this.dismiss_ = new ControllerButtonDismiss(this))
        .initialise();

      this.dismiss_.register('text-item', function (e, id, scope) {
        var item = self.items.getById(decodeURIComponent(id));

        self.callbacks.invoke("itemDismissed", item.content);
        self.items.remove(item);
      } );

      (this.items_ = new ControllerItems(this))
        .initialise();

      (this.keyboard_ = new ControllerKeyboard(this))
        .initialise();

      this.initialised_ = true;
      console.log("Sorting Queue UI initialised");
    },

    /**
     * Resets the component to a virgin state.
     *
     * @returns {Promise} Returns promise that is fulfilled upon successful
     *                    instance reset. */
    reset: function ()
    {
      var self = this;

      /* + If a reset is already underway, simply return its instance.
       * + Throw an exception if Sorting Queue has just been instantiated and is
       * currently initialising itself. */
      if(this.resetter_)
        return this.resetter_;

      this.resetter_ = new InstanceResetter(this).reset(
        [ this.requests_,
          this.keyboard_,
          this.dismiss_,
          [ this.items_,
            [
              this.callbacks_
            ]
          ]
        ] )
        .done(function () {
          self.options_ = self.callbacks_ = self.items_ = null;
          self.requests_ = self.dismiss_ = self.keyboard_ = null;

          self.initialised_ = false;

          console.log("Sorting Queue UI reset");
        } )
        .always(function () {
          self.resetter_ = false;
        } );

      return this.resetter_;
    },

    /**
     * Returns a boolean value indicating whether Sorting Queue has been
     * initialised and is ready to be used.
     *
     * @returns {Boolean}   Returns true if Sorting Queue has been successful
     *                      initialised, false otherwise.
     * */
    get initialised ()
    { return this.initialised_; },

    get resetting ()
    { return !!this.resetter_; },

    get options ()
    { return this.options_; },

    get callbacks ()
    { return this.callbacks_; },

    get requests ()
    { return this.requests_; },

    get dismiss ()
    { return this.dismiss_; },

    get items ()
    { return this.items_; },

    instantiate: function ( /* class, ... */ )
    {
      if(arguments.length < 1)
        throw "Class name required";

      var descriptor = this.options_.constructors['create' + arguments[0]];

      /* Invoke factory method to instantiate class, if it exists. */
      if(descriptor)
        return descriptor.apply(null, [].slice.call(arguments, 1));

      /* Factory method doesn't exist. Ensure class constructor has been passed
       * and instantiate it. */
      if(!this.options_.constructors.hasOwnProperty(arguments[0]))
        throw "Class or factory non existent: " + arguments[0];

      descriptor = this.options_.constructors[arguments[0]];

      /* We don't want to use `eval' so we employ a bit of trickery to
       * instantiate a class using variable arguments. */
      var fakeClass = function () { },
          object;

      /* Instantiate class prototype. */
      fakeClass.prototype = descriptor.prototype;
      object = new fakeClass();

      /* Now simply call class constructor directly and keep reference to
       * correct constructor. */
      descriptor.apply(object, [].slice.call(arguments, 1));
      object.constructor = descriptor.constructor;

      return object;
    }
  };


  /**
   * @class
   * */
  var InstanceResetter = function (instance)
  {
    this.instance_ = instance;
    this.count_ = 0;
  };

  InstanceResetter.prototype = {
    instance_: null,
    count_: null,

    reset: function (entities)
    {
      var deferred = $.Deferred();

      if(!this.instance_.initialised || this.instance_.resetting) {
        window.setTimeout(function () {
          deferred.reject();
        } );
      } else {
        var self = this;
        this.count_ = this.countEntities_(entities);

        /* Begin resetting entities. */
        this.resetEntities_(entities);

        /* Begin main instance resetting timer sequence. */
        var interval = window.setInterval(function () {
          /* Wait until all instances have reset. */
          if(self.count_)
            return;

          /* Clear interval. */
          window.clearInterval(interval);

          /* State reset. Resolve promise */
          window.setTimeout(function () {
            deferred.resolve();
          }, 10);
        }, 10);
      }

      return deferred.promise();
    },

    countEntities_: function (entities)
    {
      var self = this,
          count = 0;

      entities.forEach(function (e) {
        if(e instanceof Array)
          count += self.countEntities_(e);
        else
          ++count;
      } );

      return count;
    },

    resetEntities_: function (entities)
    {
      var self = this,
          waiting = 0;

      entities.forEach(function (e) {
        /* Deal with sub-dependencies if current element is an array. */
        if(e instanceof Array) {
          var interval = window.setInterval(function () {
            if(waiting)
              return;

            window.clearInterval(interval);
            self.resetEntities_(e);
          }, 10);
        } else {
          var result;

          try {
            result = e.reset();
          } catch(x) {
            console.log('Exception thrown whilst resetting:', x);
          }

          /* Special measure for instances that return a promise. */
          if(result && result.hasOwnProperty('always')) {
            ++waiting;

            /* Wait until it finishes. The assumption is made that instances
             * must always reset and therefore it matters not whether the
             * promise is rejected or not. */
            result.always(function () {
              --waiting;
              --self.count_;
            } );
          } else
            --self.count_;
        }
      } );
    }
  };


  /**
   * @class
   * */
  var /* abstract */ Owned = function (owner)
  {
    this.owner_ = owner;
  };

  Owned.prototype = {
    get owner ()
    { return this.owner_; }
  };


  /**
   * @class
   * */
  var /* abstract */ Controller = function (owner)
  {
    /* Invoke super constructor. */
    Owned.call(this, owner);
  };

  Controller.prototype = Object.create(Owned.prototype);

  /* Following method to allow for deferred initialisation. */
  /* abstract */ Controller.prototype.initialise = function ()
  { throw "Abstract method not implemented"; };

  /* abstract */ Controller.prototype.reset = function ()
  { throw "Abstract method not implemented"; };


  /**
   * @class
   * */
  var /* abstract */ Drawable = function (owner)
  {
    /* Invoke super constructor. */
    Owned.call(this, owner);
  };

  Drawable.prototype = Object.create(Owned.prototype);

  /* abstract */ Drawable.prototype.render = function ()
  { throw "Abstract method not implemented"; };


  /**
   * @class
   * */
  var ControllerCallbacks = function (owner, callbacks)
  {
    /* invoke super constructor. */
    Controller.call(this, owner);

    /* Set initial state. */
    this.callbacks_ = callbacks;
  };

  ControllerCallbacks.prototype = Object.create(Controller.prototype);

  ControllerCallbacks.prototype.initialise = function () { };
  ControllerCallbacks.prototype.reset = function () { };

  ControllerCallbacks.prototype.exists = function (callback)
  { return this.callbacks_.hasOwnProperty(callback); };

  ControllerCallbacks.prototype.invoke = function ()
  {
    var result = this.call_.apply(this, arguments);

    if(result && result.hasOwnProperty('always')) {
      var self = this;

      this.owner_.requests.begin(result);

      result.always(function () {
        self.owner_.requests.end(result);
      } );
    }

    return result;
  };

  ControllerCallbacks.prototype.passThrough = function ()
  {
    return this.call_.apply(this, arguments);
  };

  ControllerCallbacks.prototype.call_ = function ()
  {
    if(arguments.length < 1)
      throw "Callback name required";
    else if(!this.callbacks_.hasOwnProperty(arguments[0]))
      throw "Callback non existent: " + arguments[0];

    return this.callbacks_[arguments[0]]
      .apply(null, [].slice.call(arguments, 1));
  };


  /**
   * @class
   * */
  var ControllerRequests = function (owner)
  {
    /* invoke super constructor. */
    Controller.call(this, owner);

    /* Set initial state. */
    this.requests_ = { };
    this.count_ = 0;

    /* Define getters. */
    this.__defineGetter__("count", function () { return this.count_; } );
  };

  ControllerRequests.prototype = Object.create(Controller.prototype);

  ControllerRequests.prototype.initialise = function () {  };
  ControllerRequests.prototype.reset = function ()
  {
    var self = this,
        deferred = $.Deferred(),
        interval;

    /* Don't signal state reset until all requests processed. */
    interval = window.setInterval(function () {
      if(self.count_)
        return;

      window.clearInterval(interval);
      deferred.resolve();
    }, 10);

    return deferred.promise();
  };

  ControllerRequests.prototype.begin = function (id)
  {
    if(!this.requests_[id])
      this.requests_[id] = 1;
    else
      ++this.requests_[id];

    ++this.count_;

    /* Trigger callback. */
    if(this.owner_.callbacks.exists("onRequestStart"))
      this.owner_.callbacks.passThrough("onRequestStart", id);
  };

  ControllerRequests.prototype.end = function (id)
  {
    if(this.requests_.hasOwnProperty(id)) {
      /* Delete request from internal collection if last one, otherwise
       * decrement reference count. */
      if(this.requests_[id] == 1)
        delete this.requests_[id];
      else if(this.requests_[id] > 1)
        --this.requests_[id];
      else
        throw "Requests controller in invalid state";

      --this.count_;
    } else
      console.log("WARNING: unknown request ended:", id);

    /* Trigger callback. */
    if(this.owner_.callbacks.exists("onRequestStop"))
      this.owner_.callbacks.passThrough("onRequestStop", id);
  };


  /**
   * @class
   * */
  var ControllerKeyboardBase = function (owner)
  {
    /* Invoke super constructor. */
    Controller.call(this, owner);
  };

  ControllerKeyboardBase.prototype = Object.create(Controller.prototype);

  ControllerKeyboardBase.prototype.fnEventKeyUp = null;

  /* Required: */
  /* abstract */ ControllerKeyboardBase.prototype.onKeyUp = null;

  ControllerKeyboardBase.prototype.initialise = function ()
  {
    var self = this;

    /* Save event handler function so we are able to remove it when resetting
     * the instance. */
    this.fnEventKeyUp = function (evt) { self.onKeyUp(evt); };

    /* Set up listener for keyboard up events. */
    $('body').bind('keyup', this.fnEventKeyUp);
  };

  ControllerKeyboardBase.prototype.reset = function ()
  {
    /* Remove keyboard up event listener. */
    $('body').unbind('keyup', this.fnEventKeyUp);
    this.fnEventKeyUp = null;
  };


  /**
   * @class
   * */
  var ControllerKeyboard = function (owner)
  {
    /* Invoke super constructor. */
    ControllerKeyboardBase.call(this, owner);
  };

  ControllerKeyboard.prototype =
    Object.create(ControllerKeyboardBase.prototype);

  ControllerKeyboard.prototype.onKeyUp = function (evt)
  {
    var self = this,
        options = this.owner_.options;

    switch(evt.keyCode) {
    case options.keyboard.listUp:
      this.owner_.items.selectOffset(-1);
      break;
    case options.keyboard.listDown:
      this.owner_.items.selectOffset(1);
      break;
    case options.keyboard.listDismiss:
      this.owner_.dismiss.activate(function () {
        self.owner_.dismiss.deactivate();
      } );

      this.owner_.callbacks.invoke("itemDismissed",
                                   this.owner_.items.selected().content);
      this.owner_.items.remove();

      break;

    default:
      return;
    }

    return true;
  };


  /**
   * @class
   * */
  var ControllerButtonDismiss = function (owner)
  {
    /* Invoke super constructor. */
    Controller.call(this, owner);

    this.handlers_ = [ ];
  };

  ControllerButtonDismiss.prototype = Object.create(Controller.prototype);

  ControllerButtonDismiss.prototype.droppable_ = null;
  ControllerButtonDismiss.prototype.handlers_ = null;

  ControllerButtonDismiss.prototype.initialise = function ()
  {
    var self = this,
        options = this.owner_.options;

    this.droppable_ = new Droppable(options.nodes.buttonDismiss, {
      classHover: options.css.droppableHover,
      scopes: [ ],

      drop: function (e, id, scope) {
        if(self.handlers_.hasOwnProperty(scope)) {
          self.handlers_[scope](e, id, scope);
        } else {
          console.log("Warning: unknown scope: " + scope);
          return;
        }

        self.deactivate();

        return false;
      }
    } );
  };

  ControllerButtonDismiss.prototype.register = function (scope, fnHandler)
  {
    if(!this.droppable_)
      return;
    else if(!this.handlers_.hasOwnProperty(scope))
      this.droppable_.addScope(scope);

    this.handlers_[scope] = fnHandler;
  };

  ControllerButtonDismiss.prototype.reset = function ()
  {
    this.owner_.options.nodes.buttonDismiss.off();

    this.handlers_ = [ ];
    this.droppable_ = null;
  };

  ControllerButtonDismiss.prototype.activate = function (callback)
  {
    var options = this.owner_.options;

    options.nodes.buttonDismiss.stop().fadeIn(
      options.delays.dismissButtonShow,
      typeof callback == 'function' ? callback : null);
  };

  ControllerButtonDismiss.prototype.deactivate = function ()
  {
    var options = this.owner_.options;

    options.nodes.buttonDismiss.stop().fadeOut(
      options.delays.dismissButtonHide);
  };


  /**
   * @class
   * */
  var ControllerItems = function (owner)
  {
    /* Invoke super constructor. */
    Controller.call(this, owner);

    this.node_ = this.owner_.options.nodes.items;
    this.items_ = [ ];
    this.fnDisableEvent_ = function (e) { return false; };

    /* Define getters. */
    this.__defineGetter__("items", function () { return this.items_; } );
  };

  ControllerItems.prototype = Object.create(Controller.prototype);

  ControllerItems.prototype.initialise = function ()
  {
    /* Disallow dragging of elements over items container. */
    this.node_.on( {
      dragover: this.fnDisableEvent_
    } );

    this.check();
  };

  ControllerItems.prototype.reset = function ()
  {
    /* Reallow dragging of elements over items container. */
    this.node_.off( {
      dragover: this.fnDisableEvent_
    } );

    this.node_.children().remove();
    this.node_ = this.items_ = this.fnDisableEvent_ = null;
  };

  ControllerItems.prototype.redraw = function ()
  {
    this.node_.children().remove();
    this.items_ = [ ];
    this.check();
  };

  // Returns a de-duped `items`.
  // This includes de-duping with respect to items currently in the queue.
  ControllerItems.prototype.dedupItems = function(items) {
    var seen = {},
        deduped = [];
    for (var i = 0; i < this.items.length; i++) {
      seen[this.items[i].content.node_id] = true;
    }
    for (var i = 0; i < items.length; i++) {
      var id = items[i].node_id;
      if (!seen[id]) {
        seen[id] = true;
        deduped.push(items[i]);
      }
    }
    return deduped;
  };

  ControllerItems.prototype.check = function ()
  {
    if(this.items_.length >= this.owner_.options.visibleItems)
      return;

    var self = this;

    this.owner_.callbacks.invoke("moreTexts",
                                 this.owner_.options.visibleItems)
      .done(function (items) {
        self.owner_.requests.begin('check-items');

        /* Ensure we've received a valid items array. */
        if(items && items instanceof Array && items.length) {
          items = self.dedupItems(items);
          items.forEach(function (item, index) {
            window.setTimeout( function () {
              self.items_.push(self.owner_.instantiate('Item', self, item));
            }, Math.pow(index, 2) * 1.1);
          } );

          window.setTimeout( function () {
            self.select();
          }, 10);

          /* Ensure event is fired after the last item is added. */
          window.setTimeout( function () {
            self.owner_.requests.end('check-items');
          }, Math.pow(items.length - 1, 2) * 1.1 + 10);
        } else
          self.owner_.requests.end('check-items');
      } );
  };

  ControllerItems.prototype.select = function (variant)
  { this.select_(variant); };

  ControllerItems.prototype.selectOffset = function (offset)
  {
    var csel = this.owner_.options.css.itemSelected,
        index;

    if(!this.node_.length)
      return;
    else if(!this.node_.find('.' + csel).length) {
      this.select();
      return;
    }

    index = this.node_.find('.' + csel).prevAll().length + offset;

    if(index < 0)
      index = 0;
    else if(index > this.node_.children().length - 1)
      index = this.node_.children().length - 1;

    this.select(index);
  };

  ControllerItems.prototype.selected = function()
  {
    var node = this.getNodeSelected();

    if(!node || !node.length)
      return null;

    return this.getById(decodeURIComponent(node.attr('id')));
  };

  ControllerItems.prototype.removeAll = function() {
    for (var i = 0; i < this.items_.length; i++) {
        this.items_[i].node.remove();
    }
    this.items_ = [];
    this.check();
  };

  ControllerItems.prototype.remove = function (item)
  {
    if(typeof item == 'undefined') {
      var selected = this.selected();
      if (!selected) {
        this.check();
        return null;
      }

      return this.removeAt(this.items_.indexOf(selected));
    }

    return this.removeAt(this.items_.indexOf(item));
  };

  ControllerItems.prototype.removeAt = function (index)
  {
    if(index < 0 || index >= this.items_.length)
      throw "Invalid item index: " + index;

    var self = this,
        item = this.items_[index];

    if(item.isSelected()) {
      if(index < this.items_.length - 1)
        this.select(this.items_[index + 1]);
      else if(this.items_.length)
        this.select(this.items_[index - 1]);
      else
        console.log("No more items available");
    }

    item.node
      .css('opacity', 0.6)  /* to prevent flicker */
      .animate( { opacity: 0 },
                this.owner_.options.delays.textItemFade,
                function () {
                  $(this).slideUp(
                    self.owner_.options.delays.slideItemUp,
                    function () {
                      $(this).remove();
                      self.select();
                    } );
                } );

    this.items_.splice(index, 1);
    this.check();

    return true;
  };

  ControllerItems.prototype.getByNode = function($node) {
    return this.getById(decodeURIComponent($node.attr('id')));
  };

  ControllerItems.prototype.getById = function (id)
  {
    var result = null;

    this.items_.some(function (item) {
      if(item.content.node_id == id) {
        result = item;
        return true;
      }

      return false;
    } );

    return result;
  };

  /* overridable */ ControllerItems.prototype.getNodeSelected = function ()
  {
    return this.node_.find('.' + this.owner_.options.css.itemSelected);
  };

  /* Private methods */
  ControllerItems.prototype.select_ = function (variant,
                                                /* optional */ ev)
  {
    /* Fail silently if not initialised anymore. This might happen if, for
     * example, the `reset' method was invoked but the component is still
     * loading text items. */
    if(!this.owner_.initialised)
      return;

    var csel = this.owner_.options.css.itemSelected;

    if(!this.node_.children().length)
      return;

    if(typeof variant == 'undefined') {
      variant = this.node_.find('.' + csel);

      if(variant.length == 0)
        variant = this.node_.children().eq(0);
      else if(variant.length > 1) {
        /* We should never reach here. */
        console.log("WARNING! Multiple text items selected:", variant.length);

        variant = variant.eq(0);
      }
    } else if(typeof variant == 'number') {
      if(variant < 0)
        variant = 0;
      else if(variant > this.node_.children().length - 1)
        variant = this.node_.children().length - 1;

      variant = this.node_.children().eq(variant);
    } else if(variant instanceof Item)
      variant = variant.node;

    /* Select next item (if any), making sure currently active item (if any) is
     * deselected. */
    var current = this.getNodeSelected(),
        next = this.getByNode(variant);

    if(current.length)
      this.getByNode(current).deselect();

    if(next)
      next.select_(ev);

    /* WARNING: the present implementation requires knowledge of the list
     * items' container's height or it will fail to ensure the currently
     * selected item is always visible.
     *
     * A particular CSS style involving specifying the container's height
     * using `vh' units was found to break this behaviour.
     */

    /* Ensure text item is _always_ visible at the bottom and top ends of
     * the containing node. */
    var st = this.node_.scrollTop(),           /* scrolling top */
        ch = this.node_.innerHeight(),         /* container height */
        ipt = variant.position().top,          /* item position top */
        ih = st + ipt + variant.outerHeight(); /* item height */

    if(st + ipt < st            /* top */
       || variant.outerHeight() > ch) {
      this.node_.scrollTop(st + ipt);
    } else if(ih > st + ch) {   /* bottom */
      this.node_.scrollTop(st + ipt - ch
                           + variant.outerHeight()
                           + parseInt(variant.css('marginBottom'))
                           + parseInt(variant.css('paddingBottom')));
    }
  };


  /**
   * @class
   * */
  var Item = function (owner, item)
  {
    /* Fail silently if not initialised anymore. This might happen if, for
     * example, the `reset' method was invoked but the component is still
     * loading text items. */
    if (!owner.owner.initialised) {
        return;
    }

    /* Invoke super constructor. */
    Drawable.call(this, owner);

    this.content_ = item;
    this.node_ = null;

    /* Define getters. */
    this.__defineGetter__("content", function () { return this.content_; } );
    this.__defineGetter__("node", function () { return this.node_; } );

    this.node_ = this.render();
    this.initialise();
    owner.owner.options.nodes.items.append(this.node_);
  };

  Item.prototype = Object.create(Drawable.prototype);

  Item.prototype.initialise = function()
  {
    var self = this,
        parentOwner = this.owner_.owner;

    this.node_
      .attr( {
        id: encodeURIComponent(this.content_.node_id),
        "data-scope": "text-item"
      } )
      .click(function (ev) {
        self.owner_.select_(self, ev);
      } );

    this.getNodeClose()
      .click(function () {
        parentOwner.callbacks.invoke("itemDismissed", self.content_);
        self.owner_.remove(self);
        return false;
      } );

    /* Do not set up drag and drop on the item if not supposed to. */
    if(!parentOwner.options.itemsDraggable)
      return;

    new Draggable(this.node_, {
      classDragging: parentOwner.options.css.itemDragging,

      dragstart: function (e) {
        /* Firstly select item being dragged to ensure correct item position
         * in container. */
        self.owner_.select(self);

        /* Activate deletion/dismissal button. */
        parentOwner.dismiss.activate();
      },

      dragend: function () {
        /* Deactivate deletion/dismissal button. */
        parentOwner.dismiss.deactivate();
      }
    } );
  };

  Item.prototype.replaceNode = function (newNode)
  {
      this.node_.replaceWith(newNode);
      this.node_ = newNode;
      this.initialise();
      this.owner_.select(this);
  };

  Item.prototype.deselect = function() {
    this.node.removeClass(this.owner_.owner.options.css.itemSelected);
    this.owner_.owner.callbacks.invoke("itemDeselected", this.content);
  };

  Item.prototype.render = function() {
    var css = this.owner_.owner.options.css,
        node = $('<div class="' + css.item + '"/>'),
        content = $('<div class="' + css.itemContent + '"/>'),
        anchor = this.content_.name;

    /* Append title if existent. */
    if (this.content_.title)
      anchor += '&ndash; ' + this.content_.title;

    if(this.content_.url && anchor) {
      node.append('<a class="' + css.itemTitle + '" target="_blank" '
                  + 'href="' + this.content_.url + '">'
                  + anchor + '</a>');
    }

    node.append('<a class="' + css.itemClose + '" href="#">x</a>');

    /* Append content and remove all CSS classes from children. */
    content.append(this.content_.text);
    content.children().removeClass();
    node.append(content);
    return node;
  };

  /* Not mandatory. */
  /* overridable */
  Item.prototype.getNodeClose = function() {
    return this.node_.find('.' + this.owner_.owner.options.css.itemClose);
  };

  /* overridable */ Item.prototype.isSelected = function ()
  { return this.node_.hasClass(this.owner_.owner.options.css.itemSelected); };

  /* Private methods */
  Item.prototype.select_ = function (ev) {
    this.node.addClass(this.owner_.owner.options.css.itemSelected);
    this.owner_.owner.callbacks.invoke("itemSelected", this.content, ev);
  };


  /**
   * @class
   *
   * Static class.
   * */
  var DragDropManager = {
    activeNode_: null,

    onDragStart: function (event) {
      DragDropManager.activeNode_ = (event.originalEvent || event).target;
    },

    onDragEnd: function (event) {
      if(DragDropManager.activeNode_ == (event.originalEvent || event).target) {
        DragDropManager.activeNode_ = null;
      }
    },

    isScope: function (event, scopes)
    {
      if(!scopes)
        return true;

      var isFilter = (typeof scopes === 'function');

      if(!DragDropManager.activeNode_)
        return isFilter && scopes(null);

      var current = DragDropManager.activeNode_.getAttribute('data-scope');

      return isFilter
        ? scopes(current)
        : DragDropManager.hasScope(scopes, current);
    },

    getScope: function (event)
    {
      return DragDropManager.activeNode_
        ? DragDropManager.activeNode_.getAttribute('data-scope')
        : null;
    },

    hasScope: function (all, target)
    {
      return (all instanceof Array ? all : [ all ])
        .some(function (s) {
          return s === target;
        } );
    },

    /* Private methods */
    reset_: function ()
    { DragDropManager.activeNode_ = null; }
  };


  /**
   * @class
   * */
  var Draggable = function (node, options)
  {
    var self = this;

    node.on( {
      dragstart: function (e) {
        /* Note: event propagation needs to be stopped before assignment of
         * `originalEvent' or some tests will break. */
        e.stopPropagation();
        e = e.originalEvent;
        e.dataTransfer.setData('Text', ' ');
        e.dataTransfer.setData('DossierId', this.id);

        if(options.classDragging)
          node.addClass(options.classDragging);

        DragDropManager.onDragStart(e);

        if(options.dragstart)
          options.dragstart(e);
      },

      dragend: function (e) {
        /* Note: event propagation needs to be stopped before assignment of
         * `originalEvent' or some tests will break. */
        e.stopPropagation();
        e = e.originalEvent;

        if(options.classDragging)
          node.removeClass(options.classDragging);

        DragDropManager.onDragEnd(e);

        if(options.dragend)
          options.dragend(e);
      }
    } ).prop('draggable', true);
  };


  /**
   * @class
   * */
  var Droppable = function (node, options)
  {
    var self = this;

    this.options_ = options;
    this.node_ = node;

    node.on( {
      dragover: function (e) {
        if(!DragDropManager.isScope(e = e.originalEvent, options.scopes))
          return;

        /* Drag and drop has a tendency to suffer from flicker in the sense that
         * the `dragleave' event is fired while the pointer is on a valid drop
         * target but the `dragenter' event ISN'T fired again, causing the
         * element to lose its special styling -- given by `options.classHover'
         * -- and its `dropEffect'. We then need re-set everything in the
         * `dragover' event. */
        if(options.classHover)
          node.addClass(options.classHover);

        e.dropEffect = 'move';
        return false;
      },

      dragenter: function (e) {
        /* IE requires the following special measure. */
        if(!DragDropManager.isScope(e = e.originalEvent, options.scopes))
          return;

        e.dropEffect = 'move';
        return false;
      },

      dragleave: function (e) {
        if(!DragDropManager.isScope(e = e.originalEvent, options.scopes))
          return;

        if(options.classHover)
          node.removeClass(options.classHover);

        return false;
      },

      drop: function (e) {
        if(!DragDropManager.isScope(e = e.originalEvent, options.scopes))
          return;

        if(options.classHover)
          node.removeClass(options.classHover);

        if(options.drop) {
          /* The following try-catch is required to prevent the drop event from
           * bubbling up, should an error occur inside the handler. */
          try {
            options.drop(
              e,
              e.dataTransfer && e.dataTransfer.getData('DossierId') || null,
              DragDropManager.getScope());
          } catch (x) {
            console.log("Exception occurred:", x);
          }
        }

        /* Forcefully reset state as some drag and drop events don't cause the
         * dragleave event to be fired at the end. */
        DragDropManager.reset_();

        return false;
      }
    } );
  };

  Droppable.prototype = {
    node_: null,
    options_: null,

    addScope: function (scope)
    {
      if(!this.options_.scopes)
        this.options_.scopes = [ ];

      if(!this.options_.scopes.hasOwnProperty(scope))
        this.options_.scopes.push(scope);
    },

    reset: function ()
    {
      /* Clear all events.
       *
       * Note that this may be undesirable since all the events attached to the
       * element are cleared, including any events the client may have set
       * up. */
      this.node_.off();
      this.node_ = this.options_ = null;
    }
  };


  /* ----------------------------------------------------------------------
   *  Default options
   *  Private attribute.
   * ----------------------------------------------------------------------
   *
   * In addition to the properties below, which are obviously optional, the
   * following attributes are also accepted:
   *
   * nodes: {
   *   items: jQuery-element,           ; mandatory
   *   buttonDismiss: jQuery-element    ; optional
   * },
   * contentIds: array<string>          ; optional
   *
   */
  var defaults_ = {
    css: {
      item: 'sd-text-item',
      itemContent: 'sd-text-item-content',
      itemTitle: 'sd-text-item-title',
      itemClose: 'sd-text-item-close',
      itemSelected: 'sd-selected',
      itemDragging: 'sd-dragging'
    },
    keyboard: {                 /* Contains scan codes. */
      listUp: 38,               /* up                   */
      listDown: 40,             /* down                 */
      listDismiss: 46           /* dismiss              */
    },
    delays: {                   /* In milliseconds.     */
      animateAssign: 75,        /* Duration of assignment of text item via
                                 * shortcut. */
      slideItemUp: 150,         /* Slide up length of deleted text item. */
      textItemFade: 100         /* Fade out duration of text item after
                                 * assignment. */
    },
    constructors: {
      Item: Item
    },
    visibleItems: 20,           /* Arbitrary.           */
    binCharsLeft: 25,
    binCharsRight: 25,
    itemsDraggable: true
  };


  /**
   * Module public interface. */
  return {
    /* Base */
    Owned: Owned,
    Controller: Controller,
    Drawable: Drawable,
    ControllerKeyboardBase: ControllerKeyboardBase,

    /* Drag and drop */
    DragDropManager: DragDropManager,
    Draggable: Draggable,
    Droppable: Droppable,

    /* SortingQueue proper */
    Sorter: Sorter,
    Item: Item
  };

};


/* Compatibility with RequireJs. */
if(typeof define === "function" && define.amd) {
  define("SortingQueue", [ "jquery" ], function ($) {
    return SortingQueue_(window, $);
  });
} else
  window.SortingQueue = SortingQueue_(window, $);
