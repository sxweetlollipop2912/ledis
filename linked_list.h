// my linked-list template: https://github.com/sxweetlollipop2912/linked-list-template/
#pragma once

#define _SILENCE_ALL_CXX17_DEPRECATION_WARNINGS
#define _SILENCE_CXX17_ITERATOR_BASE_CLASS_DEPRECATION_WARNING

#include <algorithm>
#include <cstddef>
#include <functional>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <string>

template <class T>
class llnode {
public:
  T value;
  llnode<T>*prev, *next;
  
  llnode() : value{T()}, prev{nullptr}, next{nullptr} {}
  llnode(const T& x) : value{x}, prev{nullptr}, next{nullptr} {}
  llnode(const T& x, llnode<T>* const& prev, llnode<T>* const& next)
  : value{x}, prev{prev}, next{next} {}
  llnode(const llnode<T>& node)
  : value{node.value}, prev{node.prev}, next{node.next} {}
};

template <class T>
class llist {
public:
  class iterator;
  class const_iterator;
  
private:
  iterator list_begin, list_end;
  int list_size;
  
  void reset() {
    this->list_begin = this->list_end;
    this->list_end.ptr->prev = nullptr;
    this->list_size = 0;
  }
  /// Exception(s): undefined behavior: null pointer dereference
  void insert_previous(const iterator& it, const iterator& it_prev) {
    llnode<T>* node = it.ptr;
    llnode<T>* p_prev = it_prev.ptr;
    
    p_prev->next = node;
    p_prev->prev = node->prev;
    p_prev->next->prev = p_prev;
    if (p_prev->prev) p_prev->prev->next = p_prev;
    
    if (it == this->begin()) list_begin = p_prev;
    
    ++this->list_size;
  }
  /// Move nodes from a list of range [`first`, `last`) before `it`.
  /// Return iterator pointing to the first inserted value, or `pos` if
  /// `first`
  /// == `last`. Exception(s): undefined behavior: null pointer dereference
  iterator move_previous(const iterator& pos, const iterator& first,
                         const iterator& last) {
    list_size += std::distance(first, last);
    
    auto prev = pos;
    --prev;
    
    if (first != last) {
      auto node = pos.ptr;
      auto node_first = first.ptr;
      auto node_last = last.ptr->prev;
      
      node_first->prev = node->prev;
      if (node->prev) node->prev->next = node_first;
      
      node_last->next = node;
      node->prev = node_last;
      
      if (this->begin() == pos) list_begin = first;
    }
    
    return prev == nullptr ? this->begin() : ++prev;
  }
  
  /// Exception(s): out of range
  iterator get_iterator(const int& index) {
    if (index < 0 || index >= this->size()) {
      throw std::out_of_range("List of size " + std::to_string(this->size()) +
                              " is out of range at index " +
                              std::to_string(index) + ".");
    }
    
    iterator it;
    if (index < this->size() / 2) {
      it = this->begin();
      for (int i = 1; i <= index; i++) ++it;
    } else {
      it = this->end();
      for (int i = this->size() - 1; i >= index; i--) --it;
    }
    
    return it;
  }
  /// Exception(s): out of range
  const_iterator get_iterator(const int& index) const {
    if (index < 0 || index >= this->size()) {
      throw std::out_of_range("List of size " + std::to_string(this->size()) +
                              " is out of range at index " +
                              std::to_string(index) + ".");
    }
    
    const_iterator it;
    if (index < this->size() / 2) {
      it = this->begin();
      for (int i = 1; i <= index; i++) ++it;
    } else {
      it = this->end();
      for (int i = this->size() - 1; i >= index; i--) --it;
    }
    
    return it;
  }
  
public:
  llist() : list_size{0} { list_begin = list_end = new llnode<T>(); }
  llist(const std::initializer_list<T>& source) : list_size{0} {
    list_begin = list_end = new llnode<T>();
    for (const auto& element : source) this->push_back(element);
  }
  llist(const llist<T>& source) : list_size{0} {
    list_begin = list_end = new llnode<T>();
    
    for (const auto& element : source) this->push_back(element);
  }
  llist(const const_iterator& begin, const const_iterator& end) : list_size{0} {
    list_begin = list_end = new llnode<T>();
    
    for (auto it = begin; it != end; ++it) this->push_back(*it);
  }
  llist(llist<T>&& source) : list_size{0} {
    list_begin = list_end = new llnode<T>();
    move_previous(this->end(), source.begin(), source.end());
    source.reset();
  }
  ~llist() {
    this->clear();
    delete list_end.ptr;
  }
  
  bool empty() const { return this->size() == 0; }
  int length() const { return this->list_size; }
  int size() const { return this->list_size; }
  
  iterator begin() { return list_begin; }
  const_iterator begin() const { return list_begin; }
  iterator end() { return list_end; }
  const_iterator end() const { return list_end; }
  /// Allow modifications
  /// Exception(s): undefined behavior: null pointer dereference
  T& front() { return *this->begin(); }
  /// Do not allow modifications.
  /// Exception(s): undefined behavior: null pointer dereference
  const T& front() const { return *this->begin(); }
  /// Allow modifications
  /// Exception(s): undefined behavior: null pointer dereference
  T& back() {
    auto it = this->end();
    --it;
    return *it;
  }
  /// Do not allow modifications.
  /// Exception(s): undefined behavior: null pointer dereference
  const T& back() const {
    auto it = this->end();
    --it;
    return *it;
  }
  /// Allow modifications, i.e. `list[0] = sth;`
  /// Exception(s): out of range
  T& operator[](const int& index) { return *get_iterator(index); }
  /// Do not allow modifications.
  /// Exception(s): out of range
  const T& operator[](const int& index) const { return *get_iterator(index); }
  /// Allow modifications, i.e. `list.at(0) = sth;`
  /// Exception(s): out of range
  T& at(const int& index) { return (*this)[index]; }
  /// Do not allow modifications.
  /// Exception(s): out of range
  const T& at(const int& index) const { return (*this)[index]; }
  
  void push_front(const T& value) {
    auto* new_node = new llnode<T>(value);
    this->insert_previous(this->begin(), iterator(new_node));
  }
  void push_back(const T& value) {
    auto* new_node = new llnode<T>(value);
    this->insert_previous(this->end(), iterator(new_node));
  }
  /// Exception(s): out of range
  void pop_front() {
    auto it = this->begin();
    this->remove(it);
  }
  /// Exception(s): undefined behavior: null pointer dereference
  void pop_back() {
    auto it = --this->end();
    this->remove(it);
  }
  /// `value` will be at `index` in resulting list.
  /// Return iterator pointing to the inserted value
  /// Exception(s): out of range=
  iterator insert_at(const int& index, const T& value) {
    llist<T>::iterator it;
    if (index == this->size())
      it = this->end();
    else
      it = this->get_iterator(index);
    
    return this->insert(it, value);
  }
  /// Insert `value` before `pos`.
  /// Return iterator pointing to the inserted value
  /// Exception(s): undefined behavior: null pointer dereference
  iterator insert(const iterator& pos, const T& value) {
    auto it_new = iterator(new llnode<T>(value));
    insert_previous(pos, it_new);
    
    return it_new;
  }
  /// Insert count copies of `value` before `pos`.
  /// Return iterator pointing to the inserted value, or `pos` if `count` ==
  /// 0. Exception(s): undefined behavior: null pointer dereference
  iterator insert(const iterator& pos, const int& count, const T& value) {
    auto prev = pos;
    --prev;
    for (int i = 0; i < count; i++) this->insert(pos, value);
    
    return prev == nullptr ? this->begin() : ++prev;
  }
  /// `first` and `last` must not be part of this list. `last` must be
  /// reachable by `first`. Insert elements from a list of range [`first`,
  /// `last`) before `it`. Return iterator pointing to the first inserted
  /// value, or `pos` if `first` == `last`. Exception(s): undefined behavior:
  /// null pointer dereference
  iterator insert(const iterator& pos, const const_iterator& first,
                  const const_iterator& last) {
    auto prev = pos;
    --prev;
    for (auto it = first; it != last; ++it) this->insert(pos, (*it));
    
    return prev == nullptr ? this->begin() : ++prev;
  }
  /// Delete the node at `it` from list.
  /// Make sure `it` belongs to this list and not its end.
  /// Return iterator at the next element.
  /// Exception(s): undefined behavior: null pointer dereference, out of range
  iterator remove(const iterator& it) {
    if (it == this->end()) {
      throw std::out_of_range("Trying to get access to end pointer.");
    }
    
    llnode<T>* node = it.ptr;
    
    if (node->next) {
      node->next->prev = node->prev;
    }
    if (node->prev) {
      node->prev->next = node->next;
    }
    
    if (this->begin() == it) {
      ++list_begin;
    }
    
    --this->list_size;
    
    node = node->next;
    delete it.ptr;
    // it.ptr = nullptr;
    return node;
  }
  /// Return iterator at the new element at `index`.
  /// Exception(s): out of range
  iterator remove_at(const int& index) {
    auto it = this->get_iterator(index);
    return this->remove(it);
  }
  /// Return resulting size.
  int remove(const T& value, const iterator& begin, const iterator& end) {
    return this->remove_if([&](const T& element) { return element == value; },
                           begin, end);
  }
  /// Return resulting size.
  int remove(const T& value) {
    return this->remove(value, this->begin(), this->end());
  }
  /// Return resulting size.
  int remove_if(std::function<bool(const T&)> func, const iterator& begin,
                const iterator& end) {
    for (auto it = begin; it != end;)
      if (func(*it))
        it = this->remove(it);
      else
        ++it;
    
    return this->size();
  }
  /// Return resulting size.
  int remove_if(std::function<bool(const T&)> func) {
    return this->remove_if(func, this->begin(), this->end());
  }
  void resize(const int& count) {
    while (this->size() < count) this->push_back(T());
    
    while (count < this->size()) this->pop_back();
  }
  void resize(const int& count, const T& value) {
    while (this->size() < count) this->push_back(value);
    
    while (count < this->size()) this->pop_back();
  }
  /// Replace the contents with `count` copies of `value`.
  void assign(const int& count, const T& value) {
    this->resize(count);
    for (auto it = this->begin(); it != this->end(); ++it) (*it) = value;
  }
  /// `first` and `last` must not be part of this list. `last` must be reachable
  /// by `first`. Replace the contents with copies of those in the range
  /// [`first`, `last`).
  void assign(const const_iterator& first, const const_iterator& last) {
    this->resize((int)std::distance(first, last));
    
    auto it_other = first;
    for (auto it = this->begin(); it != this->end(); ++it, ++it_other)
      (*it) = (*it_other);
  }
  /// Replace the contents with the elements from the initializer list
  /// `source`.
  void assign(const std::initializer_list<T>& source) {
    this->resize((int)source.size());
    
    auto it_other = source.begin();
    for (auto it = this->begin(); it != this->end(); ++it, ++it_other)
      (*it) = (*it_other);
  }
  /// Return resulting size.
  int unique() {
    return this->unique([](const T& u, const T& v) { return u == v; });
  }
  /// Return resulting size.
  int unique(std::function<bool(const T&, const T&)> func) {
    auto last = std::unique(this->begin(), this->end(), func);
    this->resize((int)std::distance(this->begin(), last));
    
    return this->size();
  }
  void reverse_value() { std::reverse(this->begin(), this->end()); }
  void reverse() {
    if (!this->empty()) {
      for (auto it = this->begin(), it_next = this->begin(); it != this->end();
           it = it_next) {
        it_next = it;
        ++it_next;
        
        llnode<T>*& node = it.ptr;
        llnode<T>*& p_next = it_next.ptr;
        
        node->next = node->prev;
        node->prev = p_next;
      }
      
      auto new_head = this->end();
      --new_head;
      auto new_tail = this->begin();
      
      new_head.ptr->prev = nullptr;
      new_tail.ptr->next = this->end().ptr;
      list_begin = new_head;
      list_end.ptr->prev = new_tail.ptr;
    }
  }
  /// Merge 2 lists sorted in ascending order.
  void merge(llist<T>& other) {
    this->merge(other, [](const T& u, const T& v) { return u < v; });
  }
  /// Merge 2 lists sorted in ascending order.
  void merge(llist<T>& other, std::function<bool(const T&, const T&)> comp) {
    for (auto it_this = this->begin(), it_other = other.begin();
         it_other != other.end();) {
      while (it_this != this->end() && !comp(*it_other, *it_this)) ++it_this;
      
      auto nxt = it_other;
      ++nxt;
      this->insert_previous(it_this, it_other);
      it_other = nxt;
    }
    
    other.reset();
  }
  void sort() {
    this->sort([](const T& u, const T& v) { return u < v; });
  }
  void sort(std::function<bool(const T&, const T&)> comp) {
    if (this->size() > 1) {
      auto mid = this->get_iterator(this->size() / 2);
      llist<T> l1, l2;
      l1.move_previous(l1.end(), this->begin(), mid);
      l2.move_previous(l2.end(), mid, this->end());
      this->reset();
      
      l1.sort(comp);
      l2.sort(comp);
      
      this->move_previous(this->end(), l1.begin(), l1.end());
      l1.reset();
      this->merge(l2, comp);
    }
  }
  void clear() {
    if (!this->empty()) {
      for (auto it = this->begin(); it != this->end();) {
        llnode<T>* node = it.ptr;
        ++it;
        delete node;
      }
    }
    
    this->reset();
  }
  
  /// Exception(s): undefined behavior: null pointer dereference
  const_iterator find(const T& value, const const_iterator& begin,
                      const const_iterator& end) const {
    return std::find(begin, end, value);
  }
  const_iterator find(const T& value) const {
    return this->find(value, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  iterator find(const T& value, const iterator& begin, const iterator& end) {
    return std::find(begin, end, value);
  }
  iterator find(const T& value) {
    return this->find(value, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  const_iterator find_if(std::function<bool(const T&)> func,
                         const const_iterator& begin,
                         const const_iterator& end) const {
    return std::find_if(begin, end, func);
  }
  const_iterator find_if(std::function<bool(const T&)> func) const {
    return this->find_if(func, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  iterator find_if(std::function<bool(const T&)> func, const iterator& begin,
                   const iterator& end) {
    return std::find_if(begin, end, func);
  }
  iterator find_if(std::function<bool(const T&)> func) {
    return this->find_if(func, this->begin(), this->end());
  }
  int count(const T& value, const const_iterator& begin,
            const const_iterator& end) const {
    return (int)std::count(begin, end, value);
  }
  int count(const T& value) const {
    return this->count(value, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  int count_if(std::function<bool(const T&)> func, const const_iterator& begin,
               const const_iterator& end) const {
    return (int)std::count_if(begin, end, func);
  }
  int count_if(std::function<bool(const T&)> func) const {
    return this->count_if(func, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  bool all_of(std::function<bool(const T&)> func, const const_iterator& begin,
              const const_iterator& end) const {
    return std::all_of(begin, end, func);
  }
  bool all_of(std::function<bool(const T&)> func) const {
    return this->all_of(func, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  bool any_of(std::function<bool(const T&)> func, const const_iterator& begin,
              const const_iterator& end) const {
    return std::any_of(begin, end, func);
  }
  bool any_of(std::function<bool(const T&)> func) const {
    return this->any_of(func, this->begin(), this->end());
  }
  /// Exception(s): undefined behavior: null pointer dereference
  bool none_of(std::function<bool(const T&)> func, const const_iterator& begin,
               const const_iterator& end) const {
    return std::none_of(begin, end, func);
  }
  bool none_of(std::function<bool(const T&)> func) const {
    return this->none_of(func, this->begin(), this->end());
  }
  void for_each(std::function<void(const T&)> func, const const_iterator& begin,
                const const_iterator& end) const {
    std::for_each(begin, end, func);
  }
  void for_each(std::function<void(const T&)> func) const {
    this->for_each(func, this->begin(), this->end());
  }
  void for_each(std::function<void(T&)> func, const iterator& begin,
                const iterator& end) {
    std::for_each(begin, end, func);
  }
  void for_each(std::function<void(T&)> func) {
    this->for_each(func, this->begin(), this->end());
  }
  template <class TT>
  llist<TT> map(std::function<TT(const T&)> func) const {
    llist<TT> res;
    for (const auto& element : *this) res.push_back(func(element));
    return res;
  }
  llist<T> filter(std::function<bool(const T&)> func) const {
    llist<T> res;
    for (const auto& element : *this)
      if (func(element)) res.push_back(element);
    return res;
  }
  T reduce(std::function<T(const T&, const T&)> func,
           const T& initial_value) const {
    T previous_value = initial_value;
    for (const auto& current_value : *this)
      previous_value = func(previous_value, current_value);
    return previous_value;
  }
  T reduce(std::function<T(const T&, const T&)> func) const {
    T previous_value = *(this->begin());
    for (auto it = ++this->begin(); it != this->end(); ++it)
      previous_value = func(previous_value, *it);
    return previous_value;
  }
  llist<T>& operator=(const std::initializer_list<T>& source) {
    assign(source);
    return *this;
  }
  llist<T>& operator=(const llist<T>& source) {
    if (this == &source) return *this;
    assign(source.begin(), source.end());
    return *this;
  }
  llist<T>& operator=(llist<T>&& source) {
    if (this == &source) return *this;
    this->clear();
    move_previous(this->end(), source.begin(), source.end());
    source.reset();
    return *this;
  }
  void swap(llist<T>& other) {
    std::swap(this->list_begin, other.list_begin);
    std::swap(this->list_end, other.list_end);
    std::swap(this->list_size, other.list_size);
  }
};

template <class T>
class llist<T>::iterator : public std::iterator<std::bidirectional_iterator_tag,
T, std::ptrdiff_t, T*, T&> {
  friend class llist;
  
private:
  llnode<T>* ptr;
  
public:
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = T;
  using difference_type = std::ptrdiff_t;
  using pointer = T*;
  using reference = T&;
  
  iterator() : ptr{nullptr} {}
  iterator(llnode<value_type>* const& p) : ptr(p) {}
  reference operator*() const {
    reference value = ptr->value;
    return value;
  }
  /// ???? Copied from stackoverflow, not sure what it means.
  pointer operator->() const { return &*(*this); }
  /// Prefix increment
  iterator& operator++() {
    ptr = ptr->next;
    return *this;
  }
  /// Postfix increment
  iterator operator++(int) {
    iterator tmp = *this;
    ++(*this);
    return tmp;
  }
  /// Prefix decrement
  iterator& operator--() {
    ptr = ptr->prev;
    return *this;
  }
  /// Postfix decrement
  iterator operator--(int) {
    iterator tmp = *this;
    --(*this);
    return tmp;
  }
  iterator& operator=(llnode<value_type>* const& p) {
    ptr = p;
    return *this;
  }
  iterator operator+(const int& step) {
    iterator it = *this;
    if (step > 0) {
      for (int i = 0; i < step; ++i) ++it;
    }
    if (step < 0) {
      for (int i = 0; i < -step; ++i) --it;
    }
    return it;
  }
  iterator operator-(const int& step) {
    iterator it = *this;
    if (step > 0) {
      for (int i = 0; i < step; ++i) --it;
    }
    if (step < 0) {
      for (int i = 0; i < -step; ++i) ++it;
    }
    return it;
  }
  bool operator==(const iterator& it) const { return it.ptr == ptr; }
  bool operator!=(const iterator& it) const { return it.ptr != ptr; }
  void swap(iterator& other) { std::swap(this->ptr, other.ptr); }
};

template <class T>
class llist<T>::const_iterator
: public std::iterator<std::bidirectional_iterator_tag, T, std::ptrdiff_t,
T*, T&> {
  friend class llist;
  
private:
  const llnode<T>* ptr;
  
public:
  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = T;
  using difference_type = std::ptrdiff_t;
  using pointer = const T*;
  using reference = const T&;
  
  const_iterator() : ptr{nullptr} {}
  const_iterator(llnode<value_type>* const& p) : ptr(p) {}
  const_iterator(const iterator& other) : ptr(other.ptr) {}
  const_iterator(const const_iterator& other) : ptr(other.ptr) {}
  reference operator*() const {
    reference value = ptr->value;
    return value;
  }
  /// ???? Copied from stackoverflow, not sure what it means.
  pointer operator->() const { return &*(*this); }
  /// Prefix increment
  const_iterator& operator++() {
    ptr = ptr->next;
    return *this;
  }
  /// Postfix increment
  const_iterator operator++(int) {
    const_iterator tmp = *this;
    ++(*this);
    return tmp;
  }
  /// Prefix decrement
  const_iterator& operator--() {
    ptr = ptr->prev;
    return *this;
  }
  /// Postfix decrement
  const_iterator operator--(int) {
    const_iterator tmp = *this;
    --(*this);
    return tmp;
  }
  const_iterator operator+(const int& step) {
    const_iterator it = *this;
    if (step > 0) {
      for (int i = 0; i < step; ++i) ++it;
    }
    if (step < 0) {
      for (int i = 0; i < -step; ++i) --it;
    }
    return it;
  }
  const_iterator operator-(const int& step) {
    const_iterator it = *this;
    if (step > 0) {
      for (int i = 0; i < step; ++i) --it;
    }
    if (step < 0) {
      for (int i = 0; i < -step; ++i) ++it;
    }
    return it;
  }
  const_iterator& operator=(llnode<value_type>* const& p) {
    ptr = p;
    return *this;
  }
  const_iterator& operator=(const iterator& other) {
    ptr = other.ptr;
    return *this;
  }
  bool operator==(const const_iterator& it) const { return it.ptr == ptr; }
  bool operator!=(const const_iterator& it) const { return it.ptr != ptr; }
  void swap(const_iterator& other) { std::swap(this->ptr, other.ptr); }
};