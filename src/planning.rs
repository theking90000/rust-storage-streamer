use crate::StreamRequest;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalRange {
    start: u64,
    end_inclusive: u64,
}

impl PhysicalRange {
    const fn new(start: u64, end_inclusive: u64) -> Self {
        debug_assert!(start <= end_inclusive);
        Self {
            start,
            end_inclusive,
        }
    }

    pub const fn start(self) -> u64 {
        self.start
    }

    pub const fn end_inclusive(self) -> u64 {
        self.end_inclusive
    }

    pub const fn len(self) -> u64 {
        self.end_inclusive - self.start + 1
    }

    /// A planned physical range is non-empty by construction.
    pub const fn is_empty(self) -> bool {
        false
    }

    pub fn to_header_value(self) -> String {
        format!("bytes={}-{}", self.start, self.end_inclusive)
    }
}

/// One HTTP Range request. Logical bounds are absolute offsets in the
/// plaintext stream and describe exactly which part must be exposed after all
/// selected frames have been decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectReadPlan {
    pub object_index: usize,
    pub first_global_frame: u64,
    pub end_global_frame_exclusive: u64,
    pub first_frame_in_object: u32,
    pub end_frame_in_object_exclusive: u32,
    pub physical_range: PhysicalRange,
    pub logical_start: u64,
    pub logical_end_exclusive: u64,
}

impl ObjectReadPlan {
    pub const fn frame_count(&self) -> u64 {
        self.end_global_frame_exclusive - self.first_global_frame
    }
}

#[derive(Clone, Debug)]
pub struct ReadPlanner {
    plans: Vec<ObjectReadPlan>,
    logical_end_exclusive: u64,
}

impl ReadPlanner {
    pub fn new(request: &StreamRequest) -> Self {
        let payload_size = request.frame_payload_size();
        let stream_len = request
            .logical_len()
            .expect("StreamRequest validates its logical length");
        let logical_end_exclusive = request.requested_end().min(stream_len);

        if request.offset() >= logical_end_exclusive {
            return Self {
                plans: Vec::new(),
                logical_end_exclusive,
            };
        }

        let first_requested_frame = request.offset() / payload_size;
        let end_requested_frame_exclusive = logical_end_exclusive.div_ceil(payload_size);
        let mut plans = Vec::new();
        let mut object_first_global_frame = 0_u64;

        for (object_index, object) in request.objects().iter().enumerate() {
            let object_end_global_frame = object_first_global_frame + u64::from(object.frame_count);
            let first_global_frame = first_requested_frame.max(object_first_global_frame);
            let end_global_frame_exclusive =
                end_requested_frame_exclusive.min(object_end_global_frame);

            if first_global_frame < end_global_frame_exclusive {
                let first_frame_in_object = (first_global_frame - object_first_global_frame) as u32;
                let end_frame_in_object_exclusive =
                    (end_global_frame_exclusive - object_first_global_frame) as u32;
                let physical_start =
                    u64::from(first_frame_in_object) * u64::from(request.frame_size());
                let physical_end_exclusive =
                    u64::from(end_frame_in_object_exclusive) * u64::from(request.frame_size());

                plans.push(ObjectReadPlan {
                    object_index,
                    first_global_frame,
                    end_global_frame_exclusive,
                    first_frame_in_object,
                    end_frame_in_object_exclusive,
                    physical_range: PhysicalRange::new(physical_start, physical_end_exclusive - 1),
                    logical_start: request.offset().max(first_global_frame * payload_size),
                    logical_end_exclusive: logical_end_exclusive
                        .min(end_global_frame_exclusive * payload_size),
                });
            }

            object_first_global_frame = object_end_global_frame;
            if object_first_global_frame >= end_requested_frame_exclusive {
                break;
            }
        }

        Self {
            plans,
            logical_end_exclusive,
        }
    }

    pub fn plans(&self) -> &[ObjectReadPlan] {
        &self.plans
    }

    pub const fn logical_end_exclusive(&self) -> u64 {
        self.logical_end_exclusive
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ByteRate, ObjectId, ObjectMeta, StreamRequest, StreamRequestError};

    const FRAME_SIZE: u32 = 32;
    const PAYLOAD_SIZE: u64 = 16;
    const FRAMES_PER_OBJECT: u32 = 4;

    fn object(index: usize, frame_count: u32) -> ObjectMeta {
        ObjectMeta {
            id: ObjectId::new(format!("object-{index}")),
            uri: format!("objects/{index}"),
            frame_count,
        }
    }

    fn request(offset: u64, size: u64, frame_counts: &[u32]) -> StreamRequest {
        StreamRequest::new(
            Some(offset),
            size,
            frame_counts
                .iter()
                .enumerate()
                .map(|(index, count)| object(index, *count))
                .collect(),
            Some(FRAME_SIZE),
            FRAMES_PER_OBJECT,
            ByteRate::new(1_000_000).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn plans_an_unaligned_range_as_complete_frames() {
        let planner = ReadPlanner::new(&request(10, 20, &[4]));

        assert_eq!(
            planner.plans(),
            &[ObjectReadPlan {
                object_index: 0,
                first_global_frame: 0,
                end_global_frame_exclusive: 2,
                first_frame_in_object: 0,
                end_frame_in_object_exclusive: 2,
                physical_range: PhysicalRange::new(0, 63),
                logical_start: 10,
                logical_end_exclusive: 30,
            }]
        );
    }

    #[test]
    fn splits_a_read_at_object_boundaries() {
        let planner = ReadPlanner::new(&request(3 * PAYLOAD_SIZE, 3 * PAYLOAD_SIZE, &[4, 4]));

        assert_eq!(planner.plans().len(), 2);
        assert_eq!(planner.plans()[0].object_index, 0);
        assert_eq!(
            planner.plans()[0].physical_range.to_header_value(),
            "bytes=96-127"
        );
        assert_eq!(planner.plans()[1].object_index, 1);
        assert_eq!(
            planner.plans()[1].physical_range.to_header_value(),
            "bytes=0-63"
        );
        assert_eq!(planner.plans()[1].logical_end_exclusive, 6 * PAYLOAD_SIZE);
    }

    #[test]
    fn clips_a_request_to_a_short_final_object() {
        let request = request(7 * PAYLOAD_SIZE, 10 * PAYLOAD_SIZE, &[4, 4, 2]);
        let planner = ReadPlanner::new(&request);

        assert_eq!(request.logical_len(), Some(10 * PAYLOAD_SIZE));
        assert_eq!(planner.logical_end_exclusive(), 10 * PAYLOAD_SIZE);
        assert_eq!(planner.plans().len(), 2);
        assert_eq!(planner.plans()[1].object_index, 2);
        assert_eq!(
            planner.plans()[1].physical_range.to_header_value(),
            "bytes=0-63"
        );
        assert_eq!(planner.plans()[1].logical_end_exclusive, 10 * PAYLOAD_SIZE);
    }

    #[test]
    fn returns_no_plans_when_offset_is_at_or_after_eof() {
        let planner = ReadPlanner::new(&request(128, 16, &[4, 4]));
        assert!(planner.plans().is_empty());

        let planner = ReadPlanner::new(&request(129, 16, &[4, 4]));
        assert!(planner.plans().is_empty());
    }

    #[test]
    fn rejects_a_short_non_final_object() {
        let error = StreamRequest::new(
            None,
            1,
            vec![object(0, 3), object(1, 4)],
            Some(FRAME_SIZE),
            FRAMES_PER_OBJECT,
            ByteRate::new(1).unwrap(),
        )
        .unwrap_err();

        assert_eq!(
            error,
            StreamRequestError::ShortNonFinalObject {
                object_index: 0,
                frame_count: 3,
                expected: 4,
            }
        );
    }

    #[test]
    fn rejects_a_requested_range_that_overflows() {
        let error = StreamRequest::new(
            Some(u64::MAX),
            1,
            vec![object(0, 4)],
            Some(FRAME_SIZE),
            FRAMES_PER_OBJECT,
            ByteRate::new(1).unwrap(),
        )
        .unwrap_err();

        assert_eq!(error, StreamRequestError::RequestedRangeOverflow);
    }
}
