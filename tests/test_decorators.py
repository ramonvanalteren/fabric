#!/bin/env python

from fabric.decorators import *
from nose.tools import assert_true, assert_false, assert_equal


@runs_once
def single_run():
    pass

def test_runs_once():
    assert_true(is_sequential(single_run))
    assert_false(hasattr(single_run, 'has_run'))
    single_run()
    assert_true(hasattr(single_run, 'has_run'))
    assert_equal(None, single_run())


@runs_sequential
def sequential():
    pass

@runs_sequential
@runs_parallel
def sequential2():
    pass

def test_sequential():
    assert_true(is_sequential(sequential))
    assert_false(is_parallel(sequential))
    sequential()

    assert_true(is_sequential(sequential2))
    assert_false(is_parallel(sequential2))
    sequential2()


@runs_parallel
def parallel():
    pass

@runs_parallel
@runs_sequential
def parallel2():
    pass

def test_parallel():
    assert_true(is_parallel(parallel))
    assert_false(is_sequential(parallel))
    parallel() 
    
    assert_true(is_parallel(parallel2))
    assert_false(is_sequential(parallel2))
    parallel2()  


@roles('test')
def use_roles():
    pass

def test_roles():
    assert_true(hasattr(use_roles, 'roles'))
    assert_equal(use_roles.roles, ['test'])


@hosts('test')
def use_hosts():
    pass

def test_hosts():
    assert_true(hasattr(use_hosts, 'hosts'))
    assert_equal(use_hosts.hosts, ['test'])


