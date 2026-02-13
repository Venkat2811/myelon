use disruptor_mp::{SharedCursor, SharedMemoryConfig, SharedRingBuffer};
use std::sync::atomic::Ordering;

fn unique_name(prefix: &str) -> String {
    format!("{}_{}", prefix, std::process::id())
}

#[test]
fn cursor_new_or_attach_must_attach_existing_segment() {
    let (owner, name) = SharedCursor::new_auto(7).expect("owner cursor create should succeed");
    owner.store(123, Ordering::Release);

    let attached = SharedCursor::new_or_attach(&name, 0).expect("new_or_attach should succeed");

    // If new_or_attach recreated the segment, this value will be 0 instead of 123.
    assert_eq!(
        attached.load(Ordering::Acquire),
        123,
        "new_or_attach must attach existing shared cursor instead of replacing it"
    );

    attached.store(456, Ordering::Release);
    assert_eq!(
        owner.load(Ordering::Acquire),
        456,
        "owner and attached cursor must observe the same shared state"
    );
}

#[test]
fn ringbuffer_create_must_not_replace_live_segment() {
    let (owner, generated_name) =
        SharedRingBuffer::<u64>::new_auto(8, || 0).expect("owner ring create should succeed");

    unsafe {
        *owner.get(0) = 42;
    }

    let create_again_config = SharedMemoryConfig {
        name: generated_name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };

    let recreated = SharedRingBuffer::<u64>::new(create_again_config, || 0);
    assert!(
        recreated.is_err(),
        "creating a second owner on a live segment must fail (name={generated_name})"
    );

    let attach_config = SharedMemoryConfig {
        name: generated_name,
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: false,
    };

    let attached = SharedRingBuffer::<u64>::attach(attach_config)
        .expect("attach should still see original owner segment");

    let val = unsafe { *attached.get(0) };
    assert_eq!(
        val, 42,
        "attached reader should observe original segment contents, not a replaced segment"
    );
}

#[test]
fn cursor_create_with_existing_explicit_name_must_fail() {
    let name = unique_name("sc_owner");
    let owner = SharedCursor::new(&name, 11).expect("first owner create should succeed");
    owner.store(99, Ordering::Release);

    let second_owner = SharedCursor::new(&name, 0);
    assert!(
        second_owner.is_err(),
        "creating a second owner with same explicit name should fail"
    );

    let attached = SharedCursor::attach(&name).expect("attach should succeed on live owner");
    assert_eq!(
        attached.load(Ordering::Acquire),
        99,
        "attach should read original live segment"
    );
}

#[test]
fn cursor_recreate_recovers_after_simulated_crash() {
    let name = unique_name("sc_crash");

    // Simulate crash by leaking the owner handle (drop is never called).
    let owner = SharedCursor::new(&name, 10).expect("initial owner create should succeed");
    owner.store(77, Ordering::Release);
    std::mem::forget(owner);

    let second_owner = SharedCursor::new(&name, 0);
    assert!(
        second_owner.is_err(),
        "create should fail while stale/live segment exists"
    );

    let recovered = SharedCursor::recreate(&name, 123).expect("recreate should recover segment");
    assert_eq!(
        recovered.load(Ordering::Acquire),
        123,
        "recreated cursor should start with fresh initial value"
    );
}

#[test]
fn ringbuffer_recreate_recovers_after_simulated_crash() {
    let name = unique_name("srb_crash");

    let config = SharedMemoryConfig {
        name: name.clone(),
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: true,
    };

    // Simulate crash by leaking owner handle (drop is never called).
    let owner = SharedRingBuffer::<u64>::new(config.clone(), || 0)
        .expect("initial owner create should succeed");
    unsafe {
        *owner.get(0) = 9;
    }
    std::mem::forget(owner);

    let second_owner = SharedRingBuffer::<u64>::new(config.clone(), || 0);
    assert!(
        second_owner.is_err(),
        "create should fail while stale/live segment exists"
    );

    let recovered =
        SharedRingBuffer::<u64>::recreate(config, || 0).expect("recreate should recover segment");

    unsafe {
        *recovered.get(0) = 55;
    }

    let attach_config = SharedMemoryConfig {
        name,
        buffer_size: 8,
        element_size: std::mem::size_of::<u64>(),
        create: false,
    };
    let attached = SharedRingBuffer::<u64>::attach(attach_config).expect("attach should succeed");
    let value = unsafe { *attached.get(0) };
    assert_eq!(value, 55, "attached view should observe recreated segment");
}
